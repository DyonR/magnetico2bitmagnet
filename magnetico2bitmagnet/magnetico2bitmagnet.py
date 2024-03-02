#!/usr/bin/env python

__version__ = '2024.03.02a'

import sqlite3
import json
import argparse
import os

def parse_arguments():
    parser = argparse.ArgumentParser(description='magnetico2bitmagnet processes a magnetico SQLite database to extract and print data in a bitmagnet supported JSON format.', formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('database_path', nargs='?', help='The path to the SQLite database file.\nIf not provided, the script will prompt for it.')
    parser.add_argument('-o', '--output', '--to-file', help='Exports the JSON output to a file specified by this argument.')
    parser.add_argument('-s', '--split-size', type=int, help='Splits the output into multiple files after a specified number of records. Requires `--output` to be set.')
    parser.add_argument('--auto-create-dir', action='store_true', help='Automatically create the output directory if it does not exist, without prompting.')
    parser.add_argument('-v', '--version', action='version', version=f'%(prog)s {__version__}', help="Show the script's version.")
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

def ensure_directory_exists(output_file, auto_create_dir):
    """Ensures that the output directory exists."""
    directory = os.path.dirname(output_file)
    if directory and not os.path.exists(directory):
        if auto_create_dir:
            os.makedirs(directory)
        else:
            create_dir = input(f"The directory {directory} does not exist. Do you want to create it? [y/n]: ").lower()
            if create_dir == 'y':
                os.makedirs(directory)
                print(f"Directory {directory} created.")
            else:
                print("Directory creation aborted. Exiting.")
                exit(1)

def is_valid_sqlite3_file(filepath):
    """Check if the file at filepath is a valid SQLite3 database file."""
    if not os.path.isfile(filepath):
        return False
    with open(filepath, 'rb') as f:
        header = f.read(16)
    return header == b'SQLite format 3\000'

def check_database_structure(conn):
    required_columns = {"info_hash", "name", "total_size", "discovered_on"}
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='torrents'")
    if not cursor.fetchone():
        return False, "Table 'torrents' does not exist."
    
    cursor.execute("PRAGMA table_info(torrents)")
    columns = {row[1] for row in cursor.fetchall()}
    missing_columns = required_columns - columns
    if missing_columns:
        return False, f"Missing required columns: {', '.join(missing_columns)}"

    return True, ""

def generate_output_file_path(base_path, counter, split_size):
    if counter == 0:
        return base_path
    else:
        base_directory, original_filename = os.path.split(base_path)
        filename, ext = os.path.splitext(original_filename)
        new_filename = f"{filename}-{counter * split_size + 1}{ext}"
        return os.path.join(base_directory, new_filename)

def main(database_path, output_file, split_size):
    """Check if the database path exists, is readable, and is a valid SQLite3 file"""

    if not os.path.exists(database_path):
        print(f"Error: The database path '{database_path}' does not exist.")
        exit(1)
    if not os.access(database_path, os.R_OK):
        print(f"Error: The database file '{database_path}' is not accessible for reading.")
        exit(1)
    if not is_valid_sqlite3_file(database_path):
        print(f"Error: The file '{database_path}' is not a valid SQLite3 database.")
        exit(1)

    # Connect to the SQLite database
    conn = sqlite3.connect(f'file:{database_path}?mode=ro', uri=True)
    valid_structure, error_message = check_database_structure(conn)
    if not valid_structure:
        print(f"Error: {error_message}")
        conn.close()
        exit(1)

    # Set text factory to bytes since decoding is handeled by `decode_with_fallback`
    conn.text_factory = bytes
    c = conn.cursor()

    # Execute the SQL query
    c.execute("SELECT hex(info_hash), name, total_size, strftime('%Y-%m-%dT%H:%M:%S.000Z', discovered_on, 'unixepoch') FROM torrents")

    # Fetch all rows from the query
    rows = c.fetchall()

    # Close the connection
    conn.close()

    f = None
    current_record = 0
    file_counter = 0

    for row in rows:
        if output_file and (current_record % split_size == 0):
            ensure_directory_exists(output_file, args.auto_create_dir)
            if f:
                f.close()
            new_output_file = generate_output_file_path(output_file, file_counter, split_size)
            f = open(new_output_file, 'w')
            file_counter += 1

        try:
            info_hash = row[0].decode('utf-8')
            name = decode_with_fallback(row[1])
            published_at = row[3].decode('utf-8')

            data = {
                "infoHash": info_hash,
                "name": name,
                "size": row[2],
                "publishedAt": published_at,
                "source": "magnetico"
            }
            json_data = json.dumps(data, ensure_ascii=False, separators=(',', ':'))

            if output_file:
                f.write(json_data + '\n')
            else:
                print(json_data)
            current_record += 1

        except Exception as e:
            print(f"An error occurred: ", e)
            print(f"Problematic data: {row}")

    if f:
        f.close()

if __name__ == '__main__':
    args = parse_arguments()

    if not args.database_path:
        print("Error: No .sqlite3 path provided. Please provide a path.\nRefer to the `--help` to see usage.")
        exit(1)

    if args.split_size is not None and args.split_size <= 0:
        print(f"split-size must be a positive integer. '{args.split_size}' is invalid.")
        exit(1)

    main(args.database_path, args.output, args.split_size)