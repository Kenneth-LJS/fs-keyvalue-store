from logger import print_log
from typing import Any, Callable, Iterable, Mapping, Optional, Union
from collections import defaultdict
import os

from ..constants import BYTE_MAP, REVERSE_BYTE_HEX_MAP
from ..types import EntryKey, EntryKeyDeserializer, EntryKeyHasher, EntryKeySerializer, EntryKeyValue, EntryValue, EntryValueDeserializer, EntryValueResolver, EntryValueSerializer, StoreEntry, StoreKey, StoreKeyGetter, StorePath
from ..utils import get_first_value, group_into_dict, identity, resolve_sorted_iterable_duplicates
from ..fileutils import file_exists, folder_exists, get_files, get_folders, load_json_from_file, write_json_to_file

def write_entries_to_file(
    fs_path: str,
    store_path: StorePath,
    kv_entries: list[StoreEntry[EntryKey, EntryValue]],
    serialize_key: EntryKeySerializer[EntryKey] = identity,
    serialize_value: EntryValueSerializer[EntryValue] = identity,
    json_dump_args={},
):
    serialized_kv_entries = [
        {
            'key': serialize_key(entry.key.key),
            'value': serialize_value(entry.value)
        }
        for entry in kv_entries
    ]

    write_json_to_file(
        store_path.join(fs_path),
        serialized_kv_entries,
        json_dump_args,
        backup_file_path=store_path.join(fs_path, backup=True),
    )

def load_entries_from_file(
    fs_path: str,
    store_path: StorePath,
    get_store_key: StoreKeyGetter[EntryKey],
    deserialize_key: EntryKeyDeserializer[EntryKey] = identity,
    deserialize_value: EntryValueDeserializer[EntryValue] = identity,
) -> list[StoreEntry[EntryKey, EntryValue]]:
    store_file_path = store_path.join(fs_path)

    if not file_exists(store_file_path):
        backup_store_file_path = store_path.join(fs_path, backup=True)

        # Check if backup exists
        if not file_exists(backup_store_file_path):
            return []

        store_file_path = backup_store_file_path

    serialized_kv_entries = load_json_from_file(store_file_path)
    kv_entries = [
        StoreEntry(
            key=get_store_key(deserialize_key(entry['key'])),
            value=deserialize_value(entry['value']),
        )
        for entry in serialized_kv_entries
    ]
    return kv_entries


def get_store_dir_files(
    fs_path: str,
    store_path_dir: StorePath = None,
) -> list[StorePath]:
    if store_path_dir is None:
        store_path_dir = StorePath()

    if not store_path_dir.is_folder:
        raise ValueError('store_path_dir must be a folder StorePath object')

    if not folder_exists(store_path_dir.join(fs_path)):
        # Make sure the store exists
        return []

    files_as_bytes = set()
    for file in get_files(store_path_dir.join(fs_path)):
        name, ext = os.path.splitext(file)

        if ext.lower() != '.json':
            continue

        if name.startswith('_'):
            name = name[1:]

        if name == 'data':
            files_as_bytes.add(b'')
            continue

        if len(name) % 2 != 0:
            continue

        try:
            file_as_bytes = b''.join(REVERSE_BYTE_HEX_MAP[name[i:i+2]] for i in range(0, len(name), 2))
        except KeyError:
            # Invalid file name - silently skip
            continue

        # print(name, file_as_bytes)

        files_as_bytes.add(file_as_bytes)

    return [(StorePath(parents=store_path_dir.parents, name=b)) for b in files_as_bytes]


def get_store_dir_folders(
    fs_path: str,
    store_path_dir: StorePath = None,
) -> list[StorePath]:
    if store_path_dir is None:
        store_path_dir = StorePath()

    if not folder_exists(store_path_dir.join(fs_path)):
        # Make sure the store exists
        return {}

    folders = get_folders(store_path_dir.join(fs_path))
    try:
        folders_as_bytes = [REVERSE_BYTE_HEX_MAP[folder] for folder in folders]
    except KeyError:
        # Silently fail - folder is not hex
        pass
    folders_as_path = [StorePath(store_path_dir.parents + b) for b in folders_as_bytes]
    return folders_as_path


def resolve_store_iterator_duplicates(
    iter: Iterable[EntryKeyValue],
    resolve_values: EntryValueResolver[EntryKey, EntryValue] = get_first_value,
):
    def resolve_helper(key: EntryKey, buffer: list[EntryKeyValue]):
        return EntryKeyValue(
            key=key,
            value=resolve_values(key, [entry.value for entry in buffer])
        )

    buffer: list[EntryKeyValue] = []
    for item in iter:
        buffer_len = len(buffer)
        if buffer_len > 0 and buffer[0].key != item.key:
            if buffer_len == 1:
                yield buffer[0]
            else:
                yield resolve_helper(buffer)
            buffer = []

        buffer.append(item)

    buffer_len = len(buffer)
    if buffer_len == 0:
        return
    elif buffer_len == 1:
        yield buffer[0]
    else:
        yield resolve_helper(buffer)