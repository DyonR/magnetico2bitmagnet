#!/usr/bin/env python

__version__ = '2024.02.26a'

import json
import argparse
import os
import bencodepy
import hashlib
from datetime import datetime

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
            create_dir = input(f"The directory {directory} does not exist. Do you want to create it? [y/n] ").lower()
            if create_dir == 'y':
                os.makedirs(directory)
                print(f"Directory {directory} created.")
            else:
                print("Directory creation aborted. Exiting.")
                exit(1)

def generate_output_file_path(base_path, counter, split_size):
    """Generates output file path based on the counter and split size."""
    if counter == 0:
        return base_path
    else:
        base_directory, original_filename = os.path.split(base_path)
        filename, ext = os.path.splitext(original_filename)
        new_filename = f"{filename}-{counter * split_size + 1}{ext}"
        return os.path.join(base_directory, new_filename)

def get_torrent_details(torrent_path):
    """Extracts torrent details using bencodepy."""
    try:
        torrent_data = bencodepy.decode_from_file(torrent_path)
        info_dict = torrent_data[b'info']
        info_encoded = bencodepy.encode(info_dict)
        info_hash = hashlib.sha1(info_encoded).hexdigest()
        creation_date = torrent_data.get(b'creation date')
        if creation_date:
            creation_date = datetime.utcfromtimestamp(creation_date).strftime('%Y-%m-%dT%H:%M:%S.000Z')
        else:
            creation_date = 'Not available'
        total_size = 0
        if b'files' in info_dict:
            for file in info_dict[b'files']:
                total_size += file[b'length']
        else:
            total_size = info_dict[b'length']
        name = decode_with_fallback(info_dict[b'name'])
        return info_hash, name, total_size, creation_date
    except Exception as e:
        print(f"Error processing {torrent_path}: {e}")
        return None

def process_torrent_directory(directory_path, source, test_mode, output_file, split_size, auto_create_dir):
    """Processes all .torrent files in the given directory."""
    if not os.path.exists(directory_path):
        print(f"Error: The directory path '{directory_path}' does not exist.")
        exit(1)
    if not os.path.isdir(directory_path):
        print(f"Error: The path '{directory_path}' is not a directory.")
        exit(1)

    torrent_files = [os.path.join(directory_path, f) for f in os.listdir(directory_path) if f.endswith('.torrent')]
    if not torrent_files:
        print("No .torrent files found in the directory.")
        return

    f = None
    current_record = 0
    file_counter = 0

    for torrent_path in torrent_files:
        details = get_torrent_details(torrent_path)
        if details is None:
            continue
        info_hash, name, total_size, creation_date = details

        if output_file and (current_record % split_size == 0):
            ensure_directory_exists(output_file, auto_create_dir)
            if f:
                f.close()
            new_output_file = generate_output_file_path(output_file, file_counter, split_size)
            f = open(new_output_file, 'w')
            file_counter += 1

        data = {
            "infoHash": info_hash,
            "name": name,
            "size": total_size,
            "source": source
        }

        # Add 'publishedAt' only if creation_date is available
        if creation_date != "Not available":
            data["publishedAt"] = creation_date

        # Convert the dictionary to JSON
        json_data = json.dumps(data, ensure_ascii=False, separators=(',', ':'))

        if not test_mode:
            if output_file:
                f.write(json_data + '\n')
            else:
                print(json_data)
        current_record += 1

    if f:
        f.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Torrent Directory Processor: Extracts data from .torrent files in a directory to a JSON format supported by bitmagnet.', formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('directory_path', nargs='?', help='The path to the directory containing .torrent files.\nIf not provided, the script will prompt for it.')
    parser.add_argument('-t', '--test', action='store_true', help='Enables test mode: process data without printing JSON output.\nUsed to check if no encoding errors occur.')
    parser.add_argument('-o', '--to-file', help='Exports the JSON output to a file specified by this argument.')
    parser.add_argument('-s', '--split-size', type=int, help='Splits the output into multiple files after a specified number of records. Requires --to-file to be set.')
    parser.add_argument('--source', default='.torrent', help='Source tag for the output, default is ".torrent"')
    parser.add_argument('--auto-create-dir', action='store_true', help='Automatically create the output directory if it does not exist, without prompting.')
   
    parser.add_argument('-v', '--version', action='version', version=f'%(prog)s {__version__}', help="Show the script's version and exit")
    args = parser.parse_args()
    
    if args.to_file:
        if args.split_size is None:
            args.split_size = 1000000
            print("--split-size has not been set, defaulting to 1000000.")
    else:
        if args.split_size is not None:
            print("Warning: --split-size requires --to-file to be set. Ignoring --split-size.")
        args.split_size = float('inf')

    torrent_directory_path = args.directory_path if args.directory_path else input("Please enter the path to the directory containing .torrent files: ")
    process_torrent_directory(torrent_directory_path, args.source, args.test, args.to_file, args.split_size, args.auto_create_dir)
