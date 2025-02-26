#!/usr/bin/env python

__version__ = '2024.04.23b'

import argparse
import os
import sqlite3
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone
from psycopg2 import sql
from tqdm import tqdm

try:
    import pgcopy
except ImportError:
    pgcopy = None

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
    parser.add_argument('--import-padding', action='store_true', help='Handle padding files as normal files (not recommended).')
    parser.add_argument('--insert-torrent-content', "--insert-content", action='store_true', help='Insert data into the "torrent_content" column to make hashes directly searchable (not recommended).')
    parser.add_argument('--force-import', action='store_true', help='Force importing torrents with no name and filenames (not recommended).')
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


def insert_torrent_content(pg_cursor, torrents):    
    sql_command = ("INSERT INTO torrent_contents (info_hash, languages, created_at, updated_at, tsv) "
                   "VALUES %s ON CONFLICT DO NOTHING")
    try:
        psycopg2.extras.execute_values(
            pg_cursor,
            sql_command,
            [(torrent[1], '[]', torrent[5], torrent[5], torrent[1].hex()) for torrent in torrents],
            "(%s, %s, to_timestamp(%s), to_timestamp(%s), to_tsvector(%s))",
        )
    except Exception as e:
        tqdm.write(f"Error inserting torrent content into the database: {e}")
        raise


def insert_torrent_source(pg_cursor, source, torrents, copy_manager=None):
    if copy_manager is not None:
        copy_manager.threading_copy(
            [
                (source, torrent[1], *[datetime.fromtimestamp(torrent[5])] * 3)
                for torrent in torrents
            ]
        )
        return

    sql_command = (
        "INSERT INTO torrents_torrent_sources (source, info_hash, published_at, created_at, updated_at) "
        "VALUES %s ON CONFLICT DO NOTHING"
    )
    try:
        psycopg2.extras.execute_values(
            pg_cursor,
            sql_command,
            [(source, torrent[1], torrent[5], torrent[5], torrent[5]) for torrent in torrents],
            "(%s, %s, to_timestamp(%s), to_timestamp(%s), to_timestamp(%s))",
        )
    except Exception as e:
        tqdm.write(f"Error inserting torrent source into the database: {e}")
        raise


def insert_torrent_files(pg_cursor, info_hash, files_info):
    sql_command = (
        "INSERT INTO torrent_files (info_hash, index, path, size, created_at, updated_at) "
        "VALUES %s ON CONFLICT DO NOTHING"
    )
    try:
        now = datetime.now(timezone.utc)
        psycopg2.extras.execute_values(
            pg_cursor,
            sql_command,
            [(info_hash, *file_info, now, now) for file_info in files_info],
        )
    except Exception as e:
        tqdm.write(f"[ERROR]|[FILE]: Unknown error: {e}")
        raise


def get_file_details_copy(sqlite_cursor, torrents, add_files_limit, import_padding):
    now = datetime.now(timezone.utc)
    files = sqlite_cursor.fetchmany()
    seen_files = {}
    while files:
        for file in files:
            torrent_id = file[0]
            torrent_file_paths = seen_files.setdefault(torrent_id, set())
            file_index = len(torrent_file_paths)
            if file_index >= add_files_limit:
                continue
            file_path = decode_with_fallback(file[2])
            if file_path in torrent_file_paths:
                tqdm.write(f"Duplicated file path '{file_path}' in torrent {torrent_id}")
                continue
            file_size = file[1]

            if import_padding or (
                "_____padding" not in file_path and ".____padding" not in file_path
            ):
                torrent = next(
                    torrent for torrent in torrents if torrent[0] == torrent_id
                )
                yield (torrent[1], file_index, file_path, file_size, now, now)
                torrent_file_paths.add(file_path)
        files = sqlite_cursor.fetchmany()


def insert_torrent_files_copy(
    copy_manager, sqlite_conn, torrents, add_files_limit, import_padding
):
    sqlite_cursor = sqlite_conn.cursor()
    sqlite_cursor.arraysize = 1000
    sqlite_cursor.execute(
        f"SELECT torrent_id, size, path FROM files WHERE torrent_id IN ({','.join(['?'] * len(torrents))})",
        [torrent[0] for torrent in torrents],
    )

    files = get_file_details_copy(
        sqlite_cursor, torrents, add_files_limit, import_padding
    )
    copy_manager.threading_copy(files)


def insert_torrent(pg_cursor, torrents):
    sql_command = (
        "INSERT INTO torrents (info_hash, name, size, private,  created_at, updated_at, files_status, files_count) "
        "VALUES %s ON CONFLICT DO NOTHING RETURNING info_hash"
    )
    try:
        result = psycopg2.extras.execute_values(
            pg_cursor,
            sql_command,
            [torrent[1:] for torrent in torrents],
            "(%s, %s, %s, %s, to_timestamp(%s), to_timestamp(%s), %s, %s)",
            fetch=True,
        )

        return [bytes(row[0]) for row in result]
    except Exception as e:
        tqdm.write(e)
        return False


def get_torrent_details(magnetico_torrent_data, add_files, add_files_limit):
    try:
        torrent_id = magnetico_torrent_data[0]
        info_hash = magnetico_torrent_data[1]
        name = decode_with_fallback(magnetico_torrent_data[2])
        total_size = magnetico_torrent_data[3]
        creation_date = magnetico_torrent_data[4]
        files_count = magnetico_torrent_data[5] if add_files else 1
        file_status = (
            "single"
            if files_count == 1
            else "over_threshold" if files_count > add_files_limit else "multi"
        )
        if files_count == 1:
            files_count = None
    except Exception as e:
        tqdm.write(f"{e}")
    return (
        torrent_id,
        info_hash,
        name,
        total_size,
        False,
        creation_date,
        creation_date,
        file_status,
        files_count,
    )


def get_file_details(torrent_detail, add_files_limit, files, import_padding):
    (_, _, name, total_size, _, _, _, file_status, _) = torrent_detail
    try:
        files_info = []
        file_index = 0
        for file in files:
            file_path = decode_with_fallback(file[1])
            file_size = file[0]

            if (
                import_padding
                or ("_____padding" not in file_path and ".____padding" not in file_path)
                and file_index < add_files_limit
            ):
                files_info.append((file_index, file_path, file_size))
            file_index += 1
            if file_index > add_files_limit:
                break
    except Exception as e:
        tqdm.write(f"{e}")
    return files_info


def process_magnetico_database(
    database_path,
    sqlite_conn,
    pg_cursor,
    source_name,
    add_files,
    add_files_limit,
    insert_content,
    import_padding,
    force_import,
    batch_size=1000,
):
    if pgcopy is not None:
        file_copy_manager = pgcopy.CopyManager(
            pg_cursor.connection,
            "torrent_files",
            ("info_hash", "index", "path", "size", "created_at", "updated_at"),
        )
        source_copy_manager = pgcopy.CopyManager(
            pg_cursor.connection,
            "torrents_torrent_sources",
            ("source", "info_hash", "published_at", "created_at", "updated_at"),
        )
    else:
        tqdm.write("[INFO]|[Perf]: pgcopy isn't available, insertion will be slower")
        file_copy_manager = source_copy_manager = content_copy_manager = None

    sqlite_conn.text_factory = bytes
    tqdm.write("[INFO]|[SQLite]: Getting amount of records...")
    total_count = sqlite_conn.execute("SELECT COUNT(*) FROM torrents").fetchone()[0]
    tqdm.write(f"[INFO]|[SQLite]: Found {total_count} records in the database.")
    if add_files:
        torrents_query = """
                SELECT torrents.id, info_hash, name, total_size, discovered_on, count(files.torrent_id)
                FROM torrents
                LEFT JOIN files on torrents.id = files.torrent_id
                GROUP BY torrents.id
                """
    else:
        torrents_query = """
                SELECT torrents.id, info_hash, name, total_size, discovered_on
                FROM torrents
                """

    sqlite_cursor = sqlite_conn.cursor()
    sqlite_cursor.arraysize = batch_size
    torrents = sqlite_cursor.execute(torrents_query)
    files_cursor = sqlite_conn.cursor()

    with tqdm(total=total_count, desc="Processing magnetico records") as pbar:
        offset = 0
        while offset < total_count:
            batch = torrents.fetchmany()
            try:
                torrent_details = [
                    get_torrent_details(torrent, add_files, add_files_limit)
                    for torrent in batch
                ]
                inserted_hashes = insert_torrent(pg_cursor, torrent_details)
                inserted = [
                    next(
                        detail
                        for detail in torrent_details
                        if detail[1] == inserted_hash
                    )
                    for inserted_hash in inserted_hashes
                ]
                if add_files:
                    if file_copy_manager is not None:
                        insert_torrent_files_copy(
                            file_copy_manager,
                            sqlite_conn,
                            inserted,
                            add_files_limit,
                            import_padding,
                        )
                    else:
                        for inserted_torrent in inserted:
                            files = files_cursor.execute(
                                "SELECT size, path FROM files WHERE torrent_id = ?",
                                (inserted_torrent[0],),
                            )
                            file_details = get_file_details(
                                inserted_torrent, add_files_limit, files, import_padding
                            )
                            all_empty = all(
                                details[1] == "" for details in file_details
                            )
                            if all_empty:
                                if force_import:
                                    tqdm.write(
                                        f"[INFO]|[DATA]: Record with id {inserted_torrent[0]} only contains empty filenames, force importing."
                                    )
                                if not force_import:
                                    tqdm.write(
                                        f"[INFO]|[DATA]: Record with id {inserted_torrent[0]} only contains empty filenames, skipping."
                                    )
                                    continue
                            insert_torrent_files(
                                pg_cursor, inserted_torrent[1], file_details
                            )

                insert_torrent_source(pg_cursor, source_name, inserted, source_copy_manager)
                if insert_content:
                    insert_torrent_content(pg_cursor, inserted)
            finally:
                pbar.update(batch_size)
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
        tqdm.write(f"[INFO]|[SOURCE]: '{source_name}' already exists.")
        return
    
    cur = pg_conn.cursor()
    try:
        cur.execute(sql.SQL("INSERT INTO torrent_sources (key, name, created_at, updated_at) VALUES (%s, %s, %s, %s)"),
                    (source_key, source_name, timestamp_now, timestamp_now))
        pg_conn.commit()
        tqdm.write(f"[INFO]|[SOURCE]: '{source_name}' successfully added.")
    except Exception as e:
        pg_conn.rollback()
        tqdm.write(f"[INFO]|[SOURCE]: Unknown error while inserting '{source_name}': {e}")
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
        tqdm.write(f"[ERROR]|[ARGS]: --source is set to an empty string.")
        exit(1)
    if not args.add_files:
        tqdm.write(f"[INFO]|[ARGS]: --add-files is not set. Setting this is recommeneded. If you don't set this, 'mutli file' torrents will be imported as '0 files'.")
        tqdm.write(f"[INFO]|[ARGS]: The total size of a torrent will be calculated and imported either way.")
        mutli_as_zero = input(f"[INFO]|[ARGS]: Import 'mutli file' torrents as '0 files'? [y/n]: ").lower()
        if mutli_as_zero == 'y':
            tqdm.write(f"[INFO]|[ARGS]: Importing multi file torrents as '0 files'.")
        else:
            tqdm.write("[INFO]|[ARGS]: Please set --add-files to acknowledge your choice.")
            exit(1)
    if not args.insert_torrent_content:
        tqdm.write(f"[INFO]|[ARGS]: --insert-content is not set. Torrents will not show up in the WebUI until `bitmagnet reprocess` has ran.")
        tqdm.write(f"[INFO]|[ARGS]: Enabling --insert-content makes infohashes directly searchable. Either way does `bitmagnet reprocess` need to run to have them searchable.")
        no_insert_content = input(f"[INFO]|[ARGS]: Do you want to continue without inserting content? [y/n]: ").lower()
        if no_insert_content == 'y':
            tqdm.write(f"[INFO]|[ARGS]: Importing multi file torrents as '0 files'.")
        else:
            tqdm.write("[INFO]|[ARGS]: Please set --insert-content to acknowledge your choice.")
            exit(1)

    if not args.database_path:
        args.database_path = input("Enter the directory path containing database: ")
    if not os.path.exists(args.database_path):
        tqdm.write(f"[ERROR]|[FILE]: '{args.database_path}' does not exist.")
        exit(1)
    if not os.access(args.database_path, os.R_OK):
        tqdm.write(f"[ERROR]|[FILE]: '{args.database_path}' is not accessible for reading.")
        exit(1)
    if not is_valid_sqlite3_file(args.database_path):
        tqdm.write(f"[ERROR]|[FILE]:'{args.database_path}' is not a valid SQLite3 database.")
        exit(1)

    sqlite_conn = sqlite3.connect(f'file:{args.database_path}?mode=ro', uri=True)
    torrents_check_result, torrents_check_error = check_database_column_structure(sqlite_conn, "torrents", {"info_hash", "name", "total_size", "discovered_on"})
    files_check_result, files_check_error = check_database_column_structure(sqlite_conn, "files", {"id", "torrent_id", "size", "path"})
    if not torrents_check_result:
        tqdm.write(torrents_check_error)
        exit(1)
    if not files_check_result:
        tqdm.write(files_check_error)
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
    pg_cursor = pg_conn.cursor()
    try:
        pg_cursor.execute('BEGIN')
        process_magnetico_database(args.database_path, sqlite_conn, pg_cursor, args.source_name.lower(), args.add_files, args.add_files_limit, args.insert_torrent_content, args.import_padding, args.force_import)
        tqdm.write("[INFO]|[PG]: commiting…")
        pg_conn.commit()
        pg_cursor.close()
    except BaseException as e:
        tqdm.write(f"[ERROR]: Error when executing SQL: {str(e)}. Rolling back")
        pg_conn.rollback()
        import traceback

        traceback.print_exc()

if __name__ == '__main__':
    main()
