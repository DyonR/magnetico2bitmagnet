# torrent2database

`torrent2database` processes a directory (recursively) with .torrent files in it and inserts the data directory into the bitmagnet PostgreSQL database.

## Warning

When you start the script, it runs without any confirmation. I've done extensive testing to make sure everything works as expected, but I can't guarantee a 100% flawless script.  
A backup before running this script is very much recommened.  

## Requirements

The following pip packages are required:
```
bencodepy
psycopg2-binary
tqdm
charset_normalizer
```

To install these packages, you can run the command below:  
`pip install bencodepy psycopg2-binary tqdm charset_normalizer` or `pip3 install bencodepy psycopg2-binary tqdm charset_normalizer`.
  
Or, you can save the file [requirements.txt](https://raw.githubusercontent.com/DyonR/magnetico2bitmagnet/main/torrent2database/requirements.txt) and run this command:  
`pip install -r requirements.txt` or `pip3 install -r requirements.txt`

## Usage

To run the script, use the following command:

```bash
python3 torrent2database.py -h
```

### Command-line Arguments

- `<directory_path>`: The path to the directory containing .torrent files.
- `--dbname DBNAME`: bitmagnet's database name in PostgreSQL.
- `--user USER`: Username used to authenticate to PostgreSQL.
- `--password PASSWORD`: Password used to authenticate to PostgreSQL.
- `--host HOST`: PostgreSQL host.
- `--port PORT`: PostgreSQL port.
- `--source-name SOURCE_NAME`: "Torrent Source" how it will appear in bitmagnet.
- `--add-files`: Add file data to the database?
- `--add-files-limit ADD_FILES_LIMIT`: Limit the number of files to add to the database.
- `--negative-to-zero`: Torrents with a negative "size" are skipped, they make the bitmagnet WebUI unable to load. By default, torrents with a negative size are skipped.
- `--force-import-negative`: Force insert torrents with a negative size into the database (not recommended).
- `--import-padding`: Handle padding files as normal files (not recommended) (ex: `_____padding_file_0_if you see this file, please update to BitComet 0.85 or above____`).
- `-r, --recursive`: Recursively find .torrent files in subdirectories of the `<directory_path>`.


### Example

Run the command below, replacing the necessary arguments to your environment and preferences
```bash
python3 torrent2database.py /path/to/directory/with/torrent/files --dbname bitmagnet --user postgres --password PASSWORD --host 192.168.2.0 --port 5432 --source SOURCE --add-files --add-files-limit 500 --recursive
```

## Aftermath
When the import is finished (and also during the import) you cannot find torrents based on their name, only by the infohash.
After all torrents are imported, bitmagnet needs to reprocess all torrents. This process can take a very long time. Please refer to [bitmagnet's guide on how to reclassify](https://bitmagnet.io/tutorials/reprocess-reclassify.html).
