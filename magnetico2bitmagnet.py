#!/usr/bin/env python

__version__ = '2024.02.26a'

import sqlite3
import json
import argparse
import os

def decode_with_fallback(byte_sequence, encodings=('utf-8', 'cp1251', 'latin1')):
    """Attempt to decode a byte sequence using a list of encodings, falling back to a lossy decoding if necessary."""
    for encoding in encodings:
        try:
            return byte_sequence.decode(encoding)
        except UnicodeDecodeError:
            continue
    # If all decodings fail, fall back to a lossy decoding using 'utf-8' with replacement characters for undecodable bytes
    return byte_sequence.decode('utf-8', errors='replace')

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

def main(database_path, test_mode):
    """Check if the database path exists, is readable, and is a valid SQLite3 file"""
    if test_mode:
        print("Running in test mode. No JSON output will be printed.")

    if not os.path.exists(database_path):
        print(f"Error: The database path '{database_path}' does not exist.")
        exit(1)
    if not os.access(database_path, os.R_OK):
        print(f"Error: The database file '{database_path}' is not accessible for reading.")
        exit(1)
    if not is_valid_sqlite3_file(database_path):
        print(f"Error: The file '{database_path}' is not a valid SQLite3 database.")
        exit(1)

    conn = sqlite3.connect(f'file:{database_path}?mode=ro', uri=True)
    valid_structure, error_message = check_database_structure(conn)
    if not valid_structure:
        print(f"Error: {error_message}")
        conn.close()
        exit(1)

    # Connect to the SQLite database
    conn = sqlite3.connect(f'file:{database_path}?mode=ro', uri=True)
    # Set text factory to bytes since decoding is handeled by `decode_with_fallback`
    conn.text_factory = bytes
    c = conn.cursor()

    # Execute the SQL query
    c.execute("SELECT hex(info_hash), name, total_size, strftime('%Y-%m-%dT%H:%M:%S.000Z', discovered_on, 'unixepoch') FROM torrents")

    # Fetch all rows from the query
    rows = c.fetchall()

    # Close the connection
    conn.close()

    # Convert each row to a JSON object and print it
    for row in rows:
        try:
            # Decode potentially byte-encoded fields with fallbacks
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
            json_data = json.dumps(data, ensure_ascii=False)
            if not test_mode:
                print(json_data)
        except Exception as e: # If an error gets thrown, it will fail the import when piped to `curl`
            print(f"An error occurred: ", e)
            print(f"Problematic data: {row}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='magnetico2bitmagnet (m2b) processes a (magnetico) SQLite database to extract and print data in a bitmagnet supported JSON format.', formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('database_path', nargs='?', help='The path to the SQLite database file.\nIf not provided, the script will prompt for it.')
    parser.add_argument('-t', '--test', action='store_true', help='Enables test mode: process data without printing JSON output.\nUsed to check if no encoding errors occur.')
    parser.add_argument('-v', '--version', action='version', version=f'%(prog)s {__version__}', help="Show the script's version and exit")
    args = parser.parse_args()

    db_path = args.database_path if args.database_path else input("Please enter the path to the database: ")
    main(db_path, args.test)