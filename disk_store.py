"""
disk_store module implements DiskStorage class which implements the KV store on the
disk

DiskStorage provides two simple operations to get and set key value pairs. Both key and
value needs to be of string type. All the data is persisted to disk. During startup,
DiskStorage loads all the existing KV pair metadata.  It will throw an error if the
file is invalid or corrupt.

Do note that if the database file is large, then the initialisation will take time
accordingly. The initialisation is also a blocking operation, till it is completed
the DB cannot be used.

Typical usage example:

    disk: DiskStorage = DiskStore(file_name="books.db")
    disk.set(key="othello", value="shakespeare")
    author: str = disk.get("othello")
    # it also supports dictionary style API too:
    disk["hamlet"] = "shakespeare"
"""
import os.path
import time
import typing
import struct

from format import encode_kv, decode_kv, decode_header


# DiskStorage is a Log-Structured Hash Table as described in the BitCask paper. We
# keep appending the data to a file, like a log. DiskStorage maintains an in-memory
# hash table called KeyDir, which keeps the row's location on the disk.
#
# The idea is simple yet brilliant:
#   - Write the record to the disk
#   - Update the internal hash table to point to that byte offset
#   - Whenever we get a read request, check the internal hash table for the address,
#       fetch that and return
#
# KeyDir does not store values, only their locations.
#
# The above approach solves a lot of problems:
#   - Writes are insanely fast since you are just appending to the file
#   - Reads are insanely fast since you do only one disk seek. In B-Tree backed
#       storage, there could be 2-3 disk seeks
#
# However, there are drawbacks too:
#   - We need to maintain an in-memory hash table KeyDir. A database with a large
#       number of keys would require more RAM
#   - Since we need to build the KeyDir at initialisation, it will affect the startup
#       time too
#   - Deleted keys need to be purged from the file to reduce the file size
#
# Read the paper for more details: https://riak.com/assets/bitcask-intro.pdf


class DiskStorage:
    """
    Implements the KV store on the disk

    Args:
        file_name (str): name of the file where all the data will be written. Just
            passing the file name will save the data in the current directory. You may
            pass the full file location too.
    """

    def __init__(self, file_name: str = "data.db"):
        self.file_id = file_name
        self.file_handle = open(file_name, "ab+")
        self.keydir = {}
        self.offset = 0
        while self.offset < os.path.getsize(file_name):
            self.file_handle.seek(self.offset)
            header_bytes = self.file_handle.read(12)
            [timestamp, key_size, value_size] = decode_header(header_bytes)
            self.file_handle.seek(self.offset)
            kv_bytes = self.file_handle.read(12 + key_size + value_size)
            [_, key, value] = decode_kv(kv_bytes)
            key_size = len(key)
            value_size = len(value)
            self.offset = self.offset + 12 + key_size + value_size
            self.keydir[key] = {
                "file_id": file_name,
                "value_sz": value_size,
                "value_pos": self.offset - value_size,
                "tstamp": timestamp,
            }

    def set(self, key: str, value: str) -> None:
        current_timestamp = int(time.time())
        [_, kv_data] = encode_kv(timestamp=current_timestamp, key=key, value=value)
        self.file_handle.write(kv_data)
        self.file_handle.flush()
        os.fsync(self.file_handle.fileno())

        if key not in self.keydir:
            self.keydir[key] = {}
        self.keydir[key]["file_id"] = self.file_id
        self.keydir[key]["tstamp"] = current_timestamp
        self.keydir[key]["value_sz"] = len(value)
        self.keydir[key]["value_pos"] = self.file_handle.tell() - len(value)

    def get(self, key: str) -> str:
        value = ""
        if key not in self.keydir:
            return value

        metadata = self.keydir[key]
        file_path = metadata["file_id"]
        with open(file_path, "rb") as f:
            value_sz = metadata["value_sz"]
            value_pos = metadata["value_pos"]
            f.seek(value_pos)
            value_bytes = f.read(value_sz)
            [value] = struct.unpack(f"{value_sz}s", value_bytes)
        return value.decode("utf-8")

    def close(self) -> None:
        self.file_handle.flush()
        os.fsync(self.file_handle.fileno())
        self.file_handle.close()

    def __setitem__(self, key: str, value: str) -> None:
        return self.set(key, value)

    def __getitem__(self, item: str) -> str:
        return self.get(item)
