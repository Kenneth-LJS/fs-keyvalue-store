
from logger import print_log
from typing import Any, Callable, Generic, Iterable, Mapping, Optional, Union
from collections import defaultdict
import os

from ..constants import BYTE_MAP, DEFAULT_HASH_LEN, REVERSE_BYTE_HEX_MAP
from ..types import EntryKey, EntryKeyDeserializer, EntryKeyHasher, EntryKeySerializer, EntryKeyValue, EntryValue, EntryValueDeserializer, EntryValueResolver, EntryValueSerializer, StoreEntry, StoreKey, StoreKeyGetter, StorePath
from ..utils import get_first_value, group_into_dict, identity, resolve_sorted_iterable_duplicates
from ..fileutils import file_exists, folder_exists, get_files, get_folders, load_json_from_file, write_json_to_file

from .utils import get_store_dir_files, get_store_dir_folders, load_entries_from_file, resolve_store_iterator_duplicates, write_entries_to_file
from .storekey import init_get_store_key
from .iteratestore import iterate_store

# Tools

class StoreFileUtil(Generic[EntryKey, EntryValue]):
    fs_path: str
    key_hash_func: EntryKeyHasher[EntryKey]
    key_hash_len: int
    get_store_key: StoreKeyGetter[EntryKey]
    resolve_values: EntryValueResolver = get_first_value
    serialize_key: EntryKeySerializer
    deserialize_key: EntryKeyDeserializer
    serialize_value: EntryValueSerializer
    deserialize_value: EntryValueDeserializer
    json_dump_args: dict[str, Any]

    def __init__(
        self,
        fs_path: str,
        key_hash_func: EntryKeyHasher[EntryKey] = hash,
        key_hash_len: int = DEFAULT_HASH_LEN,
        resolve_values: EntryValueResolver = get_first_value,
        serialize_key: EntryKeySerializer[EntryKey] = identity,
        deserialize_key: EntryKeyDeserializer[EntryKey] = identity,
        serialize_value: EntryKeyDeserializer[EntryValue] = identity,
        deserialize_value: EntryValueDeserializer[EntryValue] = identity,
        json_dump_args: dict[str, Any] = None,
    ):
        self.fs_path = fs_path
        self.key_hash_func = key_hash_func
        self.key_hash_len = key_hash_len
        self.resolve_values = resolve_values
        self.serialize_key = serialize_key
        self.deserialize_key = deserialize_key
        self.serialize_value = serialize_value
        self.deserialize_value = deserialize_value
        self.json_dump_args = json_dump_args or None

        self.get_store_key = init_get_store_key(key_hash_func, key_hash_len)

    def exists(self, store_path: Optional[StorePath] = None, backup=False):
        if store_path is None:
            return folder_exists(self.fs_path)
        if store_path.is_file:
            return file_exists(store_path.join(self.fs_path, backup=backup))
        if store_path.is_folder:
            return folder_exists(store_path.join(self.fs_path))
            

    def join(self, store_path: StorePath, backup=False):
        return store_path.join(self.fs_path, backup=backup)

    def write_entries_to_file(
        self,
        store_path: StorePath,
        entries: list[StoreEntry[EntryKey, EntryValue]]
    ):
        write_entries_to_file(
            fs_path=self.fs_path,
            store_path=store_path,
            kv_entries=entries,
            serialize_key=self.serialize_key,
            serialize_value=self.serialize_value,
            json_dump_args=self.json_dump_args
        )

    def write_entries_to_file(
        self,
        store_path,
        entries
    ):
        write_entries_to_file(
            fs_path=self.fs_path,
            store_path=store_path,
            kv_entries=entries,
            serialize_key=self.serialize_key,
            serialize_value=self.serialize_value,
            json_dump_args=self.json_dump_args
        )

    def load_entries_from_file(self, store_path: StorePath) -> list[StoreEntry[EntryKey, EntryValue]]:
        return load_entries_from_file(
            fs_path=self.fs_path,
            store_path=store_path,
            get_store_key=self.get_store_key,
            deserialize_key=self.deserialize_key,
            deserialize_value=self.deserialize_value,
        )

    def get_files_in_dir(self, store_path_dir: StorePath) -> list[StorePath]:
        return get_store_dir_files(
            fs_path=self.fs_path,
            store_path_dir=store_path_dir,
        )

    def get_folders_in_dir(self, store_path_dir: StorePath) -> list[StorePath]:
        return get_store_dir_folders(
            fs_path=self.fs_path,
            store_path_dir=store_path_dir,
        )

    def __iter__(self):
        return iterate_store(
            fs_path=self.fs_path,
            get_store_key=self.get_store_key,
            resolve_values=self.resolve_values,
            deserialize_key=self.deserialize_key,
            deserialize_value=self.deserialize_value,
        )