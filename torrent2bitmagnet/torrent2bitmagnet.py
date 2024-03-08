#!/usr/bin/env python

__version__ = '2024.03.02a'

import json
import argparse
import os
import bencodepy
import hashlib
from datetime import datetime

def parse_arguments():
    parser = argparse.ArgumentParser(description='torrent2bitmagnet processes a directory (recursively) with .torrent files in it to extract and print data in a bitmagnet supported JSON format.', formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('directory_path', nargs='?', help='The path to the directory containing .torrent files.')
    parser.add_argument('-o', '--output', '--to-file', help='Exports the JSON output to a file specified by this argument.')
    parser.add_argument('-s', '--split-size', type=int, help='Splits the output into multiple files after a specified number of records. Requires `--output` to be set.')
    parser.add_argument('--source', default='.torrent', help='"Torrent Source" how it will appear in bitmagnet, default is ".torrent"')
    parser.add_argument('-r', '--recursive', action='store_true', help='Recursively find .torrent files in subdirectories of the <directory_path>.')
    parser.add_argument('--skip-negative', action='store_true', help='Rarely a bad .torrent can report a negative size, setting this skips those torrents.')
    parser.add_argument('--auto-create-dir', action='store_true', help='Automatically create the output directory if it does not exist, without prompting.')
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

def find_torrent_files(directory_path, recursive):
    """Finds .torrent files in the given directory, optionally searching recursively."""
    torrent_files = []
    if recursive:
        for root, dirs, files in os.walk(directory_path):
            for file in files:
                if file.endswith('.torrent'):
                    torrent_files.append(os.path.join(root, file))
    else:
        torrent_files = [os.path.join(directory_path, f) for f in os.listdir(directory_path) if f.endswith('.torrent')]
    return torrent_files

def process_torrent_directory(directory_path, source, output_file, split_size, auto_create_dir, recursive, skip_negative):
    """Processes all .torrent files in the given directory, optionally searching recursively."""
    if not os.path.exists(directory_path):
        print(f"Error: The directory path '{directory_path}' does not exist.")
        exit(1)
    if not os.path.isdir(directory_path):
        print(f"Error: The path '{directory_path}' is not a directory.")
        exit(1)

    torrent_files = find_torrent_files(directory_path, recursive)

    if not torrent_files:
        if not recursive:
            print(f"Error: No .torrent files found in '{directory_path}'.")
        else:
            print(f"Error: No .torrent files found in '{directory_path}' and the underlying directories.")
        return
            

    f = None
    current_record = 0
    file_counter = 0

    for torrent_path in torrent_files:
        details = get_torrent_details(torrent_path)
        if details is None or (skip_negative and details[2] < 0):
            continue
        if details[2] < 0:
            if skip_negative:
                continue
            else:
                temp_details = list(details)
                temp_details[2] = 0
                details = tuple(temp_details)
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

        if output_file:
            f.write(json_data + '\n')
        else:
            print(json_data)
        current_record += 1

    if f:
        f.close()

if __name__ == '__main__':
    args = parse_arguments()

    if not args.directory_path:
        print("Error: No .torrent directory path provided. Please provide a path.\nRefer to the `--help` to see usage.")
        exit(1)

    if args.split_size is not None and args.split_size <= 0:
        print(f"split-size must be a positive integer. '{args.split_size}' is invalid.")
        exit(1)

    process_torrent_directory(args.directory_path, args.source, args.output, args.split_size, args.auto_create_dir, args.recursive, args.skip_negative)
