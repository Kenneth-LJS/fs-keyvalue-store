from keyvaluestore.storeutils.storefileutil import StoreFileUtil
from logger import print_log
from typing import Any, Callable, Iterable, Mapping, Optional, Union
from collections import defaultdict
import os

from ..constants import BYTE_MAP, REVERSE_BYTE_HEX_MAP
from ..types import EntryKey, EntryKeyDeserializer, EntryKeyHasher, EntryKeySerializer, EntryKeyValue, EntryValue, EntryValueDeserializer, EntryValueResolver, EntryValueSerializer, StoreEntry, StoreKey, StoreKeyGetter, StorePath
from ..utils import get_first_value, group_into_dict, identity, resolve_sorted_iterable_duplicates
from ..fileutils import delete_file, file_exists, folder_exists, get_files, get_folders, load_json_from_file, write_json_to_file, move_file

from .utils import get_store_dir_files, get_store_dir_folders

def clean_store(sf_util: StoreFileUtil):
    if not sf_util.exists():
        # Make sure the store exists
        return

    # clean_fs_backup(fs_path)
    # inner_clean_store(
    #     fs_path=fs_path,
    #     get_store_key=get_store_key,
    #     resolve_values=resolve_values,
    #     deserialize_key=deserialize_key,
    #     deserialize_value=deserialize_value,
    # )

def clean_fs_backup(
    sf_util: StoreFileUtil,
    store_path_dir: StorePath = None,
):
    if store_path_dir is None:
        store_path_dir = StorePath()

    if not sf_util.exists(store_path_dir):
        return

    for file in sf_util.get_files_in_dir(store_path_dir):
        backup_file_path = sf_util.join(file, backup=True)
        if not file_exists(backup_file_path):
            continue

        file_path = sf_util.join(file)
        if file_exists(file_path):
            # Delete backup
            delete_file(backup_file_path)
        else:
            # Restore from backup
            move_file(backup_file_path, file_path)

    for folder in sf_util.get_folders_in_dir(store_path_dir):
        clean_fs_backup(sf_util, folder)

def cascade_fs(
    fs_path,
    get_store_key: StoreKeyGetter[EntryKey],
    serialize_key: EntryKeySerializer = identity,
    deserialize_key: EntryKeyDeserializer = identity,
    serialize_value: EntryValueSerializer = identity,
    deserialize_value: EntryValueDeserializer = identity,
    resolve_values: EntryValueResolver = get_first_value,
):
    pass



