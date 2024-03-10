#!/usr/bin/env python

__version__ = '2024.03.10a'

import argparse
import os
import sqlite3
import psycopg2
from datetime import datetime, timezone
from psycopg2 import sql
from tqdm import tqdm

def parse_arguments():
    parser = argparse.ArgumentParser(description='magnetico2database processes a magnetico SQLite database to extract and print data in a bitmagnet supported JSON format.', formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("database_path", nargs='?', help="The path to the directory containing .torrent files.\nIf not provided, the script will prompt for it.")
    parser.add_argument("--dbname", required=True, help="bitmagnet's database name in PostgreSQL.")
    parser.add_argument("--user", required=True, help="Username used to authenticate to PostgreSQL.")
    parser.add_argument("--password", required=True, help="Password used to authenticate to PostgreSQL.")
    parser.add_argument("--host", required=True, help="PostgreSQL host.")
    parser.add_argument("--port", required=True, help="PostgreSQL port.")
    parser.add_argument("--source-name", required=True, help='"Torrent Source" how it will appear in bitmagnet.')
    parser.add_argument("--add-files", action="store_true", help="Add file data to the database?")
    parser.add_argument("--add-files-limit", type=int, default=100, help="Limit the number of files to add to the database.")
    parser.add_argument("--insert-content", action="store_true", help="Directly make hashes searchable in the WebUI without the need to run `bitmagnet reprocess`")
    parser.add_argument('--import-padding', action='store_true', help='Handle padding files as normal files (not recommended).')
    parser.add_argument("-r", "--recursive", action="store_true", help='Recursively find .torrent files in subdirectories of the <directory_path>.')
    parser.add_argument('-v', '--version', action='version', version=f'%(prog)s {__version__}', help="Show the script's version and exit")
    return parser.parse_args()

def decode_with_fallback(byte_sequence, encodings=('utf-8', 'shift_jis', 'euc_jp', 'gbk', 'gb18030', 'cp1251', 'latin1')):
    """Attempt to decode a byte sequence using a list of encodings, falling back to a lossy decoding if necessary."""
    for encoding in encodings:
        try:
            return byte_sequence.decode(encoding)
        except UnicodeDecodeError:
            continue
    # If all decodings fail, fall back to a lossy decoding using 'utf-8' with replacement characters for undecodable bytes
    return byte_sequence.decode('utf-8', errors='replace')

def insert_torrent_content(pg_conn, info_hash, creation_date):
    info_hash_hex = info_hash.hex()
    tsvector_placeholder = f"'{info_hash_hex}'"
    
    sql_command = ("INSERT INTO torrent_contents (info_hash, content_type, content_source, content_id, languages, episodes, video_resolution, video_source, video_codec, video_3d, video_modifier, release_group, created_at, updated_at, tsv) "
                   "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, to_tsvector({tsvector_placeholder})) ON CONFLICT DO NOTHING")
    
    languages_json = '[]'
    values = (info_hash, None, None, None, languages_json, None, None, None, None, None, None, None, creation_date, creation_date)
    cur = pg_conn.cursor()
    try:
        cur.execute(sql.SQL(sql_command.format(tsvector_placeholder=tsvector_placeholder)), values)
        pg_conn.commit()
    except Exception as e:
        pg_conn.rollback()
        print(f"Error inserting torrent content into the database: {e}")
        print(f"Torrent source of the error: {info_hash.hex()}\n")
    finally:
        cur.close()

def insert_torrent_source(pg_conn, source, info_hash, creation_date):
    sql_command = ("INSERT INTO torrents_torrent_sources (source, info_hash, import_id, bfsd, bfpe, seeders, leechers, published_at, created_at, updated_at) "
                   "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (source, info_hash) DO NOTHING")
    values = (source, info_hash, None, None, None, None, None, creation_date, creation_date, creation_date)
    cur = pg_conn.cursor()
    try:
        cur.execute(sql.SQL(sql_command), values)
        pg_conn.commit()
    except Exception as e:
        pg_conn.rollback()
        print(f"Error inserting torrent source into the database: {e}")
        print(f"Torrent source of the error: {info_hash.hex()}\n")
    finally:
        cur.close()

def insert_torrent_files(pg_conn, info_hash, files_info):
    sql_command = ("INSERT INTO torrent_files (info_hash, index, path, size, created_at, updated_at) "
                   "VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (info_hash, path) DO NOTHING")
    cur = pg_conn.cursor()
    try:
        for file_info in files_info:
            cur.execute(sql.SQL(sql_command), (info_hash,) + file_info + (datetime.now(timezone.utc), datetime.now(timezone.utc)))
        pg_conn.commit()
    except Exception as e:
        pg_conn.rollback()
        print(f"[ERROR]|[FILE]: Unknown error: {e}")
    finally:
        cur.close()

def insert_torrent(pg_conn, torrent_details):
    sql_command = ("INSERT INTO torrents (info_hash, name, size, private, created_at, updated_at, files_status, files_count) "
                   "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (info_hash) DO NOTHING")
    cur = pg_conn.cursor()
    try:
        cur.execute(sql.SQL(sql_command), torrent_details)
        pg_conn.commit()
        return True
    except Exception as e:
        pg_conn.rollback()
        print(e)
        return False
    finally:
        cur.close()

def get_torrent_details(magnetico_torrent_data, add_files, files, import_padding, add_files_limit=10000):
    try:
        info_hash_lower = (magnetico_torrent_data[1].decode('utf-8')).lower()
        info_hash = bytes.fromhex(info_hash_lower)
        name = name = decode_with_fallback(magnetico_torrent_data[2])
        total_size = magnetico_torrent_data[3]
        creation_date = magnetico_torrent_data[4].decode('utf-8')
        file_status = 'multi'
        files_count = len(files)
        if files_count == 1:
            files_count = None
            file_status = 'single'

        files_info = []
        if file_status == "multi":
            actual_files_count = 0
            for file in files:
                file_path = decode_with_fallback(file[1])
                file_size = file[0]

                if import_padding or ("_____padding" not in file_path and ".____padding" not in file_path) and actual_files_count < add_files_limit:
                    files_info.append((actual_files_count, file_path, file_size))
                    actual_files_count += 1 
                if actual_files_count >= add_files_limit:
                    break
        else:
            if add_files:
                files_info.append((0, name, total_size))
    except Exception as e:
        print(e)
    return (info_hash, name, total_size, False, creation_date, creation_date, file_status, files_count, files_info)

def process_magnetico_database(database_path, sqlite_conn, pg_conn, source_name, add_files, add_files_limit, insert_content, import_padding, batch_size=1000):
    sqlite_conn.text_factory = bytes
    print("[INFO]|[SQLite]: Getting amount of records...")
    total_count = sqlite_conn.execute("SELECT COUNT(*) FROM torrents").fetchone()[0]
    print(f"[INFO]|[SQLite]: Found {total_count} records in the database.")

    with tqdm(total=total_count, desc="Processing magnetico records") as pbar:
        offset = 0
        while offset < total_count:
            torrents_query = f"""
            SELECT id, hex(info_hash), name, total_size, strftime('%Y-%m-%dT%H:%M:%S.000Z', discovered_on, 'unixepoch')
            FROM torrents
            LIMIT {batch_size} OFFSET {offset}
            """
            for torrent in sqlite_conn.execute(torrents_query):
                try:
                    files = []
                    files_count = 1
                    if add_files:
                        files_cursor = sqlite_conn.cursor()
                        files_cursor.execute(f"SELECT size, path FROM files WHERE torrent_id = {torrent[0]}")
                        files = files_cursor.fetchall()
                        files_count = len(files)
                        files_cursor.close()
                    torrent_details = get_torrent_details(torrent, add_files, files, import_padding, add_files_limit)
                    if torrent_details:
                        insert_torrent_succeeded = insert_torrent(pg_conn, torrent_details[:-1])
                        if not insert_torrent_succeeded:
                            continue
                        if add_files and torrent_details[-1] and torrent_details[6] != "single":
                            insert_torrent_files(pg_conn, torrent_details[0], torrent_details[-1])
                        insert_torrent_source(pg_conn, source_name, torrent_details[0], torrent_details[4])
                        if insert_content:
                            insert_torrent_content(pg_conn, torrent_details[0], torrent_details[4])
                finally:
                    pbar.update(1)
            offset += batch_size

def check_source_exists(pg_conn, source_key):
    """Check if a source key already exists in the database."""
    cur = pg_conn.cursor()
    try:
        cur.execute(sql.SQL("SELECT 1 FROM torrent_sources WHERE key = %s"), (source_key,))
        return cur.fetchone() is not None
    finally:
        cur.close()

def insert_source(pg_conn, source_name):
    """Insert a new source into the database."""
    source_key = source_name.lower()
    timestamp_now = datetime.now(timezone.utc)
    
    if check_source_exists(pg_conn, source_key):
        print(f"[INFO]|[SOURCE]: '{source_name}' already exists.")
        return
    
    cur = pg_conn.cursor()
    try:
        cur.execute(sql.SQL("INSERT INTO torrent_sources (key, name, created_at, updated_at) VALUES (%s, %s, %s, %s)"),
                    (source_key, source_name, timestamp_now, timestamp_now))
        pg_conn.commit()
        print(f"[INFO]|[SOURCE]: '{source_name}' successfully added.")
    except Exception as e:
        pg_conn.rollback()
        print(f"[INFO]|[SOURCE]: Unknown error while inserting '{source_name}': {e}")
        cur.close()
        exit(1)
    finally:
        cur.close()

def check_database_column_structure(sqlite_conn, table_name, required_columns):
    """Check if the database has all required columns"""
    cursor = sqlite_conn.cursor()
    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
    if not cursor.fetchone():
        return False, f"Table '{table_name}' does not exist."
    
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = {row[1] for row in cursor.fetchall()}
    missing_columns = required_columns - columns
    if missing_columns:
        return False, f"Missing required columns: {', '.join(missing_columns)}"
    return True, ""

def is_valid_sqlite3_file(database_path):
    """Check if the file at database_path is a valid SQLite3 database file."""
    if not os.path.isfile(database_path):
        return False
    with open(database_path, 'rb') as f:
        header = f.read(16)
    return header == b'SQLite format 3\000'

def main():
    args = parse_arguments()

    if len(args.source_name) == 0:
        print(f"[ERROR]|[ARGS]: --source is set to an empty string.")
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
    if not args.insert_content:
        print(f"[INFO]|[ARGS]: --insert-content is not set. Torrents will not show up in the WebUI until `bitmagnet reprocess` has ran.")
        print(f"[INFO]|[ARGS]: Enabling --insert-content makes infohashes directly searchable. Either way does `bitmagnet reprocess` need to run to have them searchable.")
        no_insert_content = input(f"[INFO]|[ARGS]: Do you want to continue without inserting content? [y/n]: ").lower()
        if no_insert_content == 'y':
            print(f"[INFO]|[ARGS]: Importing multi file torrents as '0 files'.")
        else:
            print("[INFO]|[ARGS]: Please set --insert-content to acknowledge your choice.")
            exit(1)

    if not args.database_path:
        args.database_path = input("Enter the directory path containing database: ")
    if not os.path.exists(args.database_path):
        print(f"[ERROR]|[FILE]: '{args.database_path}' does not exist.")
        exit(1)
    if not os.access(args.database_path, os.R_OK):
        print(f"[ERROR]|[FILE]: '{args.database_path}' is not accessible for reading.")
        exit(1)
    if not is_valid_sqlite3_file(args.database_path):
        print(f"[ERROR]|[FILE]:'{args.database_path}' is not a valid SQLite3 database.")
        exit(1)

    sqlite_conn = sqlite3.connect(f'file:{args.database_path}?mode=ro', uri=True)
    torrents_check_result, torrents_check_error = check_database_column_structure(sqlite_conn, "torrents", {"info_hash", "name", "total_size", "discovered_on"})
    files_check_result, files_check_error = check_database_column_structure(sqlite_conn, "files", {"id", "torrent_id", "size", "path"})
    if not torrents_check_result:
        print(torrents_check_error)
        exit(1)
    if not files_check_result:
        print(files_check_error)
        exit(1)

    db_params = {
        "dbname": args.dbname,
        "user": args.user,
        "password": args.password,
        "host": args.host,
        "port": args.port
    }
    pg_conn = psycopg2.connect(**db_params)
    insert_source(pg_conn, args.source_name)
    process_magnetico_database(args.database_path, sqlite_conn, pg_conn, args.source_name.lower(), args.add_files, args.add_files_limit, args.insert_content, args.import_padding)

if __name__ == '__main__':
    main()