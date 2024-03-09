#!/usr/bin/env python

__version__ = '2024.03.09c'

import argparse
import os
import bencodepy
import hashlib
import psycopg2
from psycopg2 import sql
from datetime import datetime, timezone
from pathlib import Path
from tqdm import tqdm
from charset_normalizer import from_bytes

def parse_arguments():
    parser = argparse.ArgumentParser(description="torrent2databse processes a directory (recursively) with .torrent files in it and inserts the data directory into the bitmagnet PostgreSQL database.")
    parser.add_argument("directory_path", nargs='?', help="The path to the directory containing .torrent files.\nIf not provided, the script will prompt for it.")
    parser.add_argument("--dbname", required=True, help="bitmagnet's database name in PostgreSQL.")
    parser.add_argument("--user", required=True, help="Username used to authenticate to PostgreSQL.")
    parser.add_argument("--password", required=True, help="Password used to authenticate to PostgreSQL.")
    parser.add_argument("--host", required=True, help="PostgreSQL host.")
    parser.add_argument("--port", required=True, help="PostgreSQL port.")
    parser.add_argument("--source-name", required=True, help='"Torrent Source" how it will appear in bitmagnet.')
    parser.add_argument("--add-files", action="store_true", help="Add file data to the database?")
    parser.add_argument("--add-files-limit", type=int, default=100, help="Limit the number of files to add to the database.")
    parser.add_argument('--negative-to-zero', action='store_true', help='Torrents with a negative "size" are skipped, they make the bitmagnet WebUI unable to load.\nBy default, torrents with a negative size are skipped.')
    parser.add_argument('--force-import-negative', action='store_true', help='Force insert torrents with a negative size into the database (not recommended).')
    parser.add_argument('--import-padding', action='store_true', help='Handle padding files as normal files (not recommended).')
    parser.add_argument("-r", "--recursive", action="store_true", help='Recursively find .torrent files in subdirectories of the <directory_path>.')
    parser.add_argument('-v', '--version', action='version', version=f'%(prog)s {__version__}', help="Show the script's version and exit")
    return parser.parse_args()

def decode_with_fallback(byte_sequence, preferred_encoding=None):
    matches = from_bytes(
        byte_sequence,
        cp_isolation=['utf-8', 'shift_jis', 'euc_jp', 'gbk', 'gb18030', 'cp1251', 'latin1'],
        threshold=0.2,
        language_threshold=0.1,
        enable_fallback=True
    )

    return str(matches.best())

def find_torrent_files(directory_path, recursive):
    if recursive:
        return Path(directory_path).rglob('*.torrent')
    else:
        return Path(directory_path).glob('*.torrent')

def get_torrent_details(torrent_path, add_files, add_files_limit, import_padding):
    try:
        torrent_data = bencodepy.decode_from_file(torrent_path)
        info_dict = torrent_data[b'info']
        info_encoded = bencodepy.encode(info_dict)
        info_hash = hashlib.sha1(info_encoded).digest()
        try:
            creation_date = datetime.utcfromtimestamp(torrent_data[b'creation date']).strftime('%Y-%m-%dT%H:%M:%S.000Z')
        except:
            creation_date = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.000Z')
        file_status = "single" if b'length' in info_dict else "multi"
        files_count = None
        total_size = 0
        files_info = []

        if file_status == "multi":
            files_count = len(info_dict[b'files'])
            files_info = []
            actual_files_count = 0
            total_size = sum(file[b'length'] for file in info_dict[b'files'])
            for file in info_dict[b'files']:
                file_path = os.path.join(*[decode_with_fallback(part) for part in file[b'path']])
                file_size = file[b'length']

                if import_padding or ("_____padding" not in file_path and ".____padding" not in file_path) and actual_files_count < add_files_limit:
                    files_info.append((actual_files_count, file_path, file_size))
                    actual_files_count += 1 
                if actual_files_count >= add_files_limit:
                    break
        else:
            total_size = info_dict[b'length']
            if add_files:
                name = decode_with_fallback(info_dict[b'name'])
                files_info.append((0, name, total_size))
        
        name = decode_with_fallback(info_dict[b'name'])
        return (info_hash, name, total_size, False, creation_date, creation_date, file_status, files_count, files_info)
    except Exception as e:
        if str(e) == "b'name'":
            print(f"[ERROR]|[DETAILS]: '{torrent_path}': torrent 'name' is empty.")
        else:
            print(f"[ERROR]|[DETAILS]: Unknown error '{torrent_path}': {e}")
        return None

def insert_torrent_files(conn, info_hash, files_info, torrent_path):
    sql_command = ("INSERT INTO torrent_files (info_hash, index, path, size, created_at, updated_at) "
                   "VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (info_hash, path) DO NOTHING")
    cur = conn.cursor()
    try:
        for file_info in files_info:
            cur.execute(sql.SQL(sql_command), (info_hash,) + file_info + (datetime.now(timezone.utc), datetime.now(timezone.utc)))
        conn.commit()
    except Exception as e:
        conn.rollback()
        if str(e) ==  'A string literal cannot contain NUL (0x00) characters.':
            print(f"[ERROR]|[FILE]: '{torrent_path}': filelist contains empty or invalid names.")
        else:
            print(f"[ERROR]|[FILE]: Unknown error {torrent_path}': {e}")
    finally:
        cur.close()

def insert_torrent(conn, torrent_details, torrent_path):
    sql_command = ("INSERT INTO torrents (info_hash, name, size, private, created_at, updated_at, files_status, files_count) "
                   "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (info_hash) DO NOTHING")
    cur = conn.cursor()
    try:
        cur.execute(sql.SQL(sql_command), torrent_details)
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        if str(e) == "A string literal cannot contain NUL (0x00) characters.":
            print(f"[ERROR]|[TORRENT]: '{torrent_path}': torrent 'name' is an invalid string.")
        else:
            print(f"[ERROR]|[TORRENT]: Unknown error'{torrent_path}': {e}")
        return False
    finally:
        cur.close()

def insert_torrent_source(conn, source, info_hash, creation_date):
    sql_command = ("INSERT INTO torrents_torrent_sources (source, info_hash, import_id, bfsd, bfpe, seeders, leechers, published_at, created_at, updated_at) "
                   "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (source, info_hash) DO NOTHING")
    values = (source, info_hash, None, None, None, None, None, creation_date, creation_date, creation_date)
    cur = conn.cursor()
    try:
        cur.execute(sql.SQL(sql_command), values)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error inserting torrent source into the database: {e}")
        print(f"Torrent source of the error: {info_hash.hex()}\n")
    finally:
        cur.close()

def insert_torrent_content(conn, info_hash, creation_date):
    info_hash_hex = info_hash.hex()
    tsvector_placeholder = f"'{info_hash_hex}'"
    
    sql_command = ("INSERT INTO torrent_contents (info_hash, content_type, content_source, content_id, languages, episodes, video_resolution, video_source, video_codec, video_3d, video_modifier, release_group, created_at, updated_at, tsv) "
                   "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, to_tsvector({tsvector_placeholder})) ON CONFLICT DO NOTHING")
    
    languages_json = '[]'
    values = (info_hash, None, None, None, languages_json, None, None, None, None, None, None, None, creation_date, creation_date)
    cur = conn.cursor()
    try:
        cur.execute(sql.SQL(sql_command.format(tsvector_placeholder=tsvector_placeholder)), values)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error inserting torrent content into the database: {e}")
        print(f"Torrent source of the error: {info_hash.hex()}\n")
    finally:
        cur.close()


def check_source_exists(conn, source_key):
    """Check if a source key already exists in the database."""
    cur = conn.cursor()
    try:
        cur.execute(sql.SQL("SELECT 1 FROM torrent_sources WHERE key = %s"), (source_key,))
        return cur.fetchone() is not None
    finally:
        cur.close()

def insert_source(conn, source_name):
    """Insert a new source into the database."""
    source_key = source_name.lower()
    timestamp_now = datetime.now(timezone.utc)
    
    if check_source_exists(conn, source_key):
        print(f"[INFO]|[SOURCE]: '{source_name}' already exists.")
        return
    
    cur = conn.cursor()
    try:
        cur.execute(sql.SQL("INSERT INTO torrent_sources (key, name, created_at, updated_at) VALUES (%s, %s, %s, %s)"),
                    (source_key, source_name, timestamp_now, timestamp_now))
        conn.commit()
        print(f"[INFO]|[SOURCE]: '{source_name}' successfully added.")
    except Exception as e:
        conn.rollback()
        print(f"[INFO]|[SOURCE]: Unknown error while inserting '{source_name}': {e}")
        exit(1)
    finally:
        cur.close()


def process_torrent_files(directory_path, recursive, conn, source_name, add_files, add_files_limit, negative_to_zero, force_import_negative, import_padding):
    torrent_paths = list(find_torrent_files(directory_path, recursive))
    with tqdm(total=len(torrent_paths), desc="Processing Torrent Files") as pbar:
        for torrent_path in torrent_paths:
            torrent_details = get_torrent_details(torrent_path, add_files, add_files_limit, import_padding)
            if None == torrent_details:
                pbar.update(1)
                continue
            if not force_import_negative and (torrent_details[:-1][2] < 0): # If the torrent size is negative
                if negative_to_zero:
                    print(f"[INFO]|[SIZE]: '{torrent_path}' 'size' value is '{torrent_details[:-1][2]}', setting it to '0'.")
                    torrent_details = torrent_details[:2] + (0,) + torrent_details[3:]
                else:
                    print(f"[ERROR]|[SIZE]: '{torrent_path}' 'size' value is '{torrent_details[:-1][2]}', not importing.")
                    pbar.update(1)
                    continue
            else:
                if force_import_negative and (torrent_details[:-1][2] < 0):
                    print(f"[INFO]|[SIZE]: {torrent_path}' 'size' value is '{torrent_details[:-1][2]}', force importing.")
            if torrent_details:
                insert_torrent_succeeded = insert_torrent(conn, torrent_details[:-1], torrent_path)  # Exclude files_info from torrent_details
                if not insert_torrent_succeeded:
                    pbar.update(1)
                    continue
                # Only run insert_torrent_files if file_status is not 'single' and there are files to insert
                if add_files and torrent_details[-1] and torrent_details[6] != "single":
                    insert_torrent_files(conn, torrent_details[0], torrent_details[-1], torrent_path)
                insert_torrent_source(conn, source_name, torrent_details[0], torrent_details[4])
                insert_torrent_content(conn, torrent_details[0], torrent_details[4])
            pbar.update(1)

def main():
    args = parse_arguments()
    if len(args.source_name) == 0:
        print(f"[ERROR]|[ARGS]: --source is set to an empty string.")
        exit(1)
    if args.negative_to_zero and args.force_import_negative:
        print(f"[ERROR]|[ARGS]: --negative-to-zero and --force-import-negative may not be used together.")
        exit(1)
    if not args.add_files:
        print(f"[INFO]|[ARGS]: --add-files is not set. Setting this is recommeneded. If you don't set this, 'mutli file' torrents will be imported as '0 files'.")
        print(f"[INFO]|[ARGS]: The total size of a torrent will be calculated and imported either way.")
        mutli_as_zero = input(f"[INFO]|[ARGS]: Import 'mutli file' torrents as '0 files'? [y/n]: ").lower()
        if mutli_as_zero == 'y':
            print(f"[INFO]|[ARGS]: Importing multi file torrents as '0 files'.")
        else:
            print("[INFO]|[ARGS]: Please set --add-files to acknowledge your choice.")
            exit(1)
    if not args.directory_path:
        args.directory_path = input("Enter the directory path containing .torrent files: ")

    db_params = {
        "dbname": args.dbname,
        "user": args.user,
        "password": args.password,
        "host": args.host,
        "port": args.port
    }
    conn = psycopg2.connect(**db_params)
    insert_source(conn, args.source_name)
    source_key = args.source_name.lower()
    process_torrent_files(args.directory_path, args.recursive, conn, source_key, args.add_files, args.add_files_limit, args.negative_to_zero, args.force_import_negative, args.import_padding)

    if conn:
        conn.close()

if __name__ == "__main__":
    main()