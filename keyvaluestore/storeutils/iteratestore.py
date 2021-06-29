from logger import print_log
from typing import Any, Callable, Iterable, Mapping, Optional, Union
from collections import defaultdict
import os

from ..constants import BYTE_MAP, REVERSE_BYTE_HEX_MAP
from ..types import EntryKey, EntryKeyDeserializer, EntryKeyHasher, EntryKeySerializer, EntryKeyValue, EntryValue, EntryValueDeserializer, EntryValueResolver, EntryValueSerializer, StoreEntry, StoreKey, StoreKeyGetter, StorePath
from ..utils import get_first_value, group_into_dict, identity, resolve_sorted_iterable_duplicates
from ..fileutils import file_exists, folder_exists, get_files, get_folders, load_json_from_file, write_json_to_file

from .utils import get_store_dir_files, load_entries_from_file, resolve_store_iterator_duplicates

def iterate_store(
    fs_path: str,
    get_store_key: StoreKeyGetter[EntryKey],
    resolve_values: EntryValueResolver = get_first_value,
    deserialize_key: EntryKeyDeserializer[EntryKey] = identity,
    deserialize_value: EntryValueDeserializer[EntryValue] = identity,
):
    if not folder_exists(fs_path):
        # Make sure the store exists
        return

    inner_iter = inner_iterate_store(
        fs_path=fs_path,
        get_store_key=get_store_key,
        deserialize_key=deserialize_key,
        deserialize_value=deserialize_value,
    )

    inner_iter = resolve_store_iterator_duplicates(
        inner_iter,
        resolve_values=resolve_values,
    )

    for store_entry in inner_iter:
        yield EntryKeyValue(
            key=store_entry.key.key,
            value=store_entry.value,
        )

def inner_iterate_store(
    fs_path: str,
    get_store_key: StoreKeyGetter[EntryKey],
    store_path_dir: StorePath = None,
    related_store_paths: list[StorePath] = None,
    related_entries: list[StoreEntry[EntryKey, EntryValue]] = None,
    deserialize_key: EntryKeyDeserializer[EntryKey] = identity,
    deserialize_value: EntryValueDeserializer[EntryValue] = identity,
):
    if store_path_dir is None:
        store_path_dir = StorePath()
    if related_store_paths is None:
        related_store_paths = []
    if related_entries is None:
        related_entries = []

    if not folder_exists(store_path_dir.join(fs_path)):
        # Bottom of the directory
        # Let's load up everything
        for related_store_path in related_store_paths:
            related_entries += load_entries_from_file(
                fs_path=fs_path,
                store_path=related_store_path,
                get_store_key=get_store_key,
                deserialize_key=deserialize_key,
                deserialize_value=deserialize_value,
            )
        related_entries.sort(key=lambda entry: entry.key.hash)

        for related_entry in related_entries:
            yield related_entry

        return
    
    store_path_dir_len = len(store_path_dir)

    sub_files_dict = group_into_dict(
        lambda store_path: store_path.byte_path[store_path_dir_len:store_path_dir_len+1],
        get_store_dir_files(fs_path, store_path_dir) + related_store_paths
    )

    # Bottomed-out files -> load them up into memory
    for related_store_path in sub_files_dict[b'']:
        related_entries += load_entries_from_file(
            fs_path=fs_path,
            store_path=related_store_path,
            get_store_key=get_store_key,
            deserialize_key=deserialize_key,
            deserialize_value=deserialize_value,
        )

    sub_entries_dict = group_into_dict(
        lambda entry: entry.key.hashb[store_path_dir_len:store_path_dir_len+1],
        related_entries,
    )

    for b in BYTE_MAP:
        sub_path = store_path_dir + b

        folder_iter = inner_iterate_store(
            fs_path=fs_path,
            get_store_key=get_store_key,
            store_path_dir=sub_path,
            related_store_paths=sub_files_dict[b],
            related_entries=sub_entries_dict[b],
            deserialize_key=deserialize_key,
            deserialize_value=deserialize_value,
        )

        for entry in folder_iter:
            yield entry
