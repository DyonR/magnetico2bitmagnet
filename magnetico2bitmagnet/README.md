# magnetico2bitmagnet

`magnetico2bitmagnet` processes a magnetico SQLite database to extract and print data in a bitmagnet-supported JSON format.

## Usage

To run the script, use the following command:

```bash
python3 magnetico2bitmagnet.py <path_to_database>
```

### Command-line Arguments

- `<path_to_database>`: The path to your magnetico SQLite database file.
- `-o`, `--output`, `--to-file`: Exports the JSON output to a file specified by this argument.
- `-s`, `--split-size`: Splits the output into multiple files after a specified number of records. Requires `--output` to be set.
- `--auto-create-dir`: Automatically creates the output directory if it does not exist, without prompting.
- `--skip-negative`: Rarely a bad .torrent can report a negative size, setting this skips those torrents. If not passed, it will be imported with the reported negative size.
- `-v`, `--version`: Displays the script's version.
- `-h`, `--help`: Shows the help message.

### Examples

Run the script with a specified database path:
```bash
python3 magnetico2bitmagnet.py /path/to/magnetico/database.sqlite3
```

Run the script, export the output, and limit the contents of a file to `100000`:
```bash
python3 magnetico2bitmagnet.py /path/to/magnetico/database.sqlite3 --output /path/to/your/output/database.json --split-size 100000
```

### Importing to bitmagnet

To import data to bitmagnet, utilize the `/import` endpoint of bitmagnet.  
Run the command below, replacing `/path/to/magnetico/database.sqlite3` with the path to your database and `192.168.2.0:3333` with your bitmagnet instance's IP address and port, or hostname and port.

1. Start the import:
```bash
python3 magnetico2bitmagnet.py /path/to/magnetico/database.sqlite3 | curl --verbose -H "Content-Type: application/json" -H "Connection: close" --data-binary @- http://192.168.2.0:3333/import
```

**Note**: Importing millions of records may use **a lot of RAM** and will also take a really long time to import. This can take **hours**.  
After importing, bitmagnet will also classify all imported torrents before they appear in the WebUI. This can take **multiple days**.
