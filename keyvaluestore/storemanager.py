from typing import Generic, Optional
import json
import os
import sys

# from .keyhasher import KeyHasher, DEFAULT_HASH_SIZE, create_hash_byte_func
from .types import EntryKey, EntryValue, EntryKeyHasher, EntryKeyByteHasher, EntryKeySerializer, EntryKeyDeserializer, EntryValueResolver, EntryValueSerializer, EntryValueDeserializer, StoreKeyGetter
from .storeaccessor import StoreAccessor
from .storeentry import StoreEntry, StoreKey
from .utils import deserialize_store_entry, get_first_value, serialize_store_entry, identity
from .fileutils import mkdir_for_path
from .storeutils import init_get_store_key
from .constants import DEFAULT_HASH_LEN

class StoreManager(Generic[EntryKey, EntryValue]): # Temp implementation
    fs_path: str
    max_files_open: int = 1
    key_hash_func: EntryKeyHasher[EntryKey] = hash,
    key_hash_len: int = DEFAULT_HASH_LEN,
    serialize_key: EntryKeySerializer[EntryKey]
    deserialize_key: EntryKeyDeserializer[EntryKey]
    serialize_value: EntryValueSerializer[EntryValue]
    deserialize_value: EntryValueDeserializer[EntryValue]
    resolve_values: EntryValueResolver[EntryKey, EntryValue]
    json_dump_args: dict[str, any]

    get_store_key: StoreKeyGetter
    stored_entries: dict[EntryKey, StoreEntry]

    def __init__(
        self,
        fs_path: str,
        max_files_open: int = 1,
        key_hash_func: EntryKeyHasher[EntryKey] = hash,
        key_hash_len: int = DEFAULT_HASH_LEN,
        serialize_key: EntryKeySerializer[EntryKey] = identity,
        deserialize_key: EntryKeyDeserializer[EntryKey] = identity,
        serialize_value: EntryValueSerializer[EntryValue] = identity,
        deserialize_value: EntryValueDeserializer[EntryValue] = identity,
        resolve_values: EntryValueResolver[EntryKey, EntryValue] = get_first_value,
        json_dump_args: Optional[dict[str, any]] = None,
    ):
        if json_dump_args is None:
            json_dump_args = {}

        if 'obj' in json_dump_args:
            raise ValueError(f'Key "obj" not allowed in "json_dump_args"')
        elif 'fp' in json_dump_args:
            raise ValueError(f'Key "fp" not allowed in "json_dump_args"')

        self.fs_path = fs_path
        self.max_files_open = max_files_open
        self.key_hash_func = key_hash_func
        self.key_hash_len = key_hash_len
        self.serialize_key = serialize_key
        self.deserialize_key = deserialize_key
        self.serialize_value = serialize_value
        self.deserialize_value = deserialize_value
        self.resolve_values = resolve_values
        self.json_dump_args = json_dump_args

        self.get_store_key = init_get_store_key(key_hash_func, key_hash_len)
        self.stored_entries = {}

    def load_all_entries(self) -> dict[EntryKey, StoreEntry]:
        loaded_entries: dict[EntryKey, StoreEntry] = {}

        if not os.path.isfile(self._get_json_path()):
            return loaded_entries
        
        serialized_entries = []
        with open(self._get_json_path(), 'r', encoding='utf-8') as f:
            serialized_entries = json.load(f)
            
        for serialized_entry in serialized_entries:
            entry = deserialize_store_entry(serialized_entry, self.deserialize_key, self.deserialize_value)
            loaded_entries[entry.key] = entry

        return loaded_entries

    def write_all_entries(self):
        serialized_entries = []
        for entry in self.stored_entries.values():
            serialized_entry = serialize_store_entry(entry, self.serialize_key, self.serialize_value)
            serialized_entries.append(serialized_entry)

        mkdir_for_path(self._get_json_path())
        with open(self._get_json_path(), 'w', encoding='utf-8') as f:
            json.dump(serialized_entries, f, **self.json_dump_args)

    def flush(self):
        self.write_all_entries()

    def has_key(self, key: EntryValue) -> bool:
        return key in self.stored_entries

    def get_value(self, key: EntryKey) -> EntryValue:
        return self.stored_entries[key].value

    def set_value(self, key: EntryKey, value: EntryValue):
        self.stored_entries[key] = StoreEntry(
            key=key,
            value=value
        )

    def remove_key(self, key: EntryKey):
        del self.stored_entries[key]

    def _get_json_path(self):
        return os.path.join(self.fs_path, 'data.json')

    def open(self):
        self.stored_entries = self.load_all_entries()
        return StoreAccessor(self)

    def close(self):
        self.flush()

    def __enter__(self):
        return self.open()

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __setitem__(self, key: EntryKey, value: EntryValue):
        self.set_value(key, value)

    def __getitem__(self, key: EntryKey) -> EntryValue:
        return self.get_value(key)

    def __repr__(self):
        return super(self).__repr__(self)
        # return repr(self._dict)

    def __len__(self):
        raise NotImplementedError()
        # return len(self._dict)

    def __delitem__(self, key: EntryKey):
        self.remove_key(key)

    def clear(self):
        raise NotImplementedError()
        # return self._dict.clear()

    def copy(self):
        raise NotImplementedError()
        # return self._dict.copy()

    def update(self, *args, **kwargs):
        raise NotImplementedError()
        # return self._dict.update(*args, **kwargs)

    def keys(self):
        raise NotImplementedError()
        # return self._dict.keys()

    def values(self):
        raise NotImplementedError()
        # return self._dict.values()

    def items(self):
        raise NotImplementedError()
        # return self._dict.items()

    def pop(self, *args):
        raise NotImplementedError()
        # return self._dict.pop(*args)

    def popitem(self, *args):
        raise NotImplementedError()
        # return self._dict.popitem(*args)

    # def __cmp__(self, dict):
    #     return self._dict.__cmp__(dict)

    def __contains__(self, key: EntryKey):
        return self.has_key(key)

    def __iter__(self):
        raise NotImplementedError()
        # return iter(self._dict)

