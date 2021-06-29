from logger import print_log
from typing import Any, Callable, Iterable, Mapping, Optional, Union
from collections import defaultdict
import os

from ..constants import BYTE_MAP, REVERSE_BYTE_HEX_MAP
from ..types import EntryKey, EntryKeyDeserializer, EntryKeyHasher, EntryKeySerializer, EntryKeyValue, EntryValue, EntryValueDeserializer, EntryValueResolver, EntryValueSerializer, StoreEntry, StoreKey, StoreKeyGetter, StorePath
from ..utils import get_first_value, group_into_dict, identity, resolve_sorted_iterable_duplicates
from ..fileutils import file_exists, folder_exists, get_files, get_folders, load_json_from_file, write_json_to_file

def init_get_store_key(hash_func: EntryKeyHasher[EntryKey], hash_len: int) -> StoreKeyGetter[EntryKey]:
    if (hash_len & 7) != 0:
        raise ValueError('hash_len is not a multiple of 8')

    hash_len_minus_1 = hash_len - 1
    hash_len_byte_len = hash_len >> 3

    add_diff = 1 << hash_len_minus_1

    def get_store_key(key: EntryKey) -> StoreKey[EntryKey]:
        hash_val = hash_func(key)
        hash_bytes = (hash_func(key) + add_diff).to_bytes(hash_len_byte_len, 'big', signed=False)
        return StoreKey[EntryKey](
            key=key,
            hash=hash_val,
            hashb=hash_bytes,
        )

    return get_store_key
