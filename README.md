# magnetico2bitmagnet
magnetico2bitmagnet processes a (magnetico) SQLite database to extract and print data in a bitmagnet supported JSON format

## Features
- Converts and prints torrent data in a JSON format suitable for bitmagnet's import endpoint.
- Verifies database integrity and structure before processing.
- Optional test mode for validation without output.
- Only imports the following per record:
  - infohash
  - name
  - size
  - discovered on (as 'published at', currently not visible in bitmagnet's WebUI)
  - source (as magnetico)
  - files per torrent are **NOT** imported due to a limition of bitmagnet's import endpoint

## Usage

To run the script, use the following command:

```
python3 magnetico2bitmagnet.py <path_to_database>
```

If the path to the database is not provided, the script will prompt you to enter it interactively.

### Command-line Arguments

- `<path_to_database>`: The path to your magnetico SQLite database file. If not provided, the script will ask for it.
- `-t`, `--test`: Run the script in test mode to check for potential errors without printing the JSON data.
- `-v`, `--version`: Display the script's version.
- `-h`, `--help`: Show the help message and exit.

### Examples

Run the script with a specified database path:
```
python3 magnetico2bitmagnet.py /path/to/magnetico/database.sqlite3
```

Run the script in test mode:
```
python3 magnetico2bitmagnet.py --test /path/to/magnetico/database.sqlite3
```

### Importing to bitmagnet

Before importing your data into bitmagnet, it's advised to run the Python script with the `--test` flag to validate that there are no invalid records in your database.

To import data to bitmagnet, the script utilizes the `/import` endpoint.  
Follow these steps, replacing `/path/to/magnetico/database.sqlite3` with the path to your database and `192.168.2.0:3333` with your bitmagnet instance's IP address or hostname and port.

1. Validate your data:
```
python3 magnetico2bitmagnet.py --test /path/to/magnetico/database.sqlite3
```
2. Upon successful validation (if not errors are printed), start the import:
```
python3 magnetico2bitmagnet.py /path/to/magnetico/database.sqlite3 | curl --verbose -H "Content-Type: application/json" -H "Connection: close" --data-binary @- http://192.168.2.0:3333/import
```
**Note**: Importing millions of records will use **a lot of RAM** and will also take a really long time to import. This can take **hours**.  
After importing, bitmagnet will also classify all imported torrents before they appear in the WebUI. This can take **multiple days**.

## Acknowledgments

- Thanks to the developers of [magnetico](https://github.com/boramalper/magnetico) and [bitmagnet](https://github.com/bitmagnet-io/bitmagnet) for their excellent work on making self-hosted DHT crawlers accessible.
