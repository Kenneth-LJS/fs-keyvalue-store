from keyvaluestore.constants import ALL_BYTE_HEX_STR
import random
from dataclasses import dataclass
import os
from time import perf_counter
from contextlib import contextmanager

from lib import *

from testdatamaker import *
from keyvaluestore.storeutils import *
from keyvaluestore.fileutils import *
from keyvaluestore.utils import *
from keyvaluestore.types import *

from memory_profiler import profile # pip install memory_profiler

@profile
def run_read_iter():
    store_name = 'test_2'
    fs_path = get_fs_path(store_name)

    get_store_key = init_get_store_key(
        hash_word_key,
        HASH_WORD_KEY_LEN
    )

    counter = 0
    for entry in iterate_store(
        fs_path=fs_path,
        get_store_key=get_store_key,
        deserialize_key=deserialize_word_key,
        deserialize_value=deserialize_word_value,
    ):
        counter += 1

@profile
def run_read_load():
    store_name = 'test_2'
    fs_path = get_fs_path(store_name)

    get_store_key = init_get_store_key(
        hash_word_key,
        HASH_WORD_KEY_LEN
    )

    counter = 0
    for entry in load_brute_force(
        fs_path=fs_path,
        get_store_key=get_store_key,
        deserialize_key=deserialize_word_key,
        deserialize_value=deserialize_word_value,
    ):
        counter += 1

@contextmanager
def print_time(task_name):
    start = perf_counter()
    yield
    diff_in_seconds = perf_counter() - start
    print(f"{task_name}: {diff_in_seconds:.4f} secs")

def test_time():
    return False

    # if True:
    #     tests = [
    #         ('ITER', run_read_iter),
    #         ('LOAD', run_read_load),
    #     ]

    #     for test_name, test_func in tests:
    #         print(f'TEST: {test_name}')
    #         with print_time(test_name):
    #             test_func()

    #         profile(test_func)()

    if True:
        print('Test: ITER')
        run_read_iter()

    if True:
        print('Test: LOAD')
        run_read_load()

    return True

def main():
    # test_time()
    # test_1()
    test_2()
    # test_3()
    # test_4()

def test_1():
    # Simple files store - malformed structure
    store_name = 'test_1'
    if not store_exists(store_name):
        make_data_1(store_name)
    test_iterate(store_name)

def test_2():
    # Dense file store - malformed structure
    store_name = 'test_2'
    if not store_exists(store_name):
        make_data_2(store_name)
    test_iterate(store_name)

def test_3():
    # Simple files store - malformed structure + backup files
    store_name = 'test_3'
    if not store_exists(store_name):
        make_data_3(store_name)
    test_iterate(store_name)

def test_4():
    # Simple files store - everything has backup
    # Let's cleanup

    store_name = 'test_4'
    if not store_exists(store_name):
        make_data_4(store_name)
    test_iterate(store_name)

    store_util = get_word_store_util(get_fs_path(store_name))

    clean_fs_backup(store_util)


def test_iterate(store_name):
    fs_path = get_fs_path(store_name)

    store_util = get_word_store_util(fs_path)

    with print_time('ITER'):
        iterated_keys = [
            entry.key.word
            for entry in iter(store_util)
        ]

    with print_time('LOAD'):
        loaded_keys = [
            entry.key.word
            for entry in load_brute_force(
                    fs_path=fs_path,
                    get_store_key=store_util.get_store_key,
                    resolve_values=resolve_word_values,
                    deserialize_key=deserialize_word_key,
                    deserialize_value=deserialize_word_value,
                )
        ]

    # with open('iter.txt', 'w', encoding='utf-8') as f:
    #     for k in iterated_keys:
    #         f.write(k + '\n')
    # with open('brute.txt', 'w', encoding='utf-8') as f:
    #     for k in loaded_keys:
    #         f.write(k + '\n')

    print('LEN', len(iterated_keys), len(loaded_keys))
    iterated_keys.sort()
    loaded_keys.sort()
    is_equal = iterated_keys == loaded_keys
    print('EQUAL', is_equal)

    if len(iterated_keys) == len(loaded_keys) and not is_equal:
        for k, (i_key, l_key) in enumerate(zip(iterated_keys, loaded_keys)):
            if i_key != l_key:
                print(k, i_key, l_key)

    print('done')

def load_brute_force(
    fs_path: str,
    get_store_key: StoreKeyGetter[EntryKey],
    resolve_values: EntryValueResolver = get_first_value,
    deserialize_key: EntryKeyDeserializer[EntryKey] = identity,
    deserialize_value: EntryValueDeserializer[EntryValue] = identity,
) -> EntryKeyValue[EntryKey, EntryValue]:
    
    store_entries = load_brute_force_inner(
        fs_path=fs_path,
        get_store_key=get_store_key,
        deserialize_key=deserialize_key,
        deserialize_value=deserialize_value,
    )

    store_entries.sort(key=lambda store_entry: store_entry.key.hash)

    kv_entries = resolve_store_iterator_duplicates(
        (
            EntryKeyValue(store_entry.key.key, store_entry.value)
            for store_entry in store_entries
        ),
        resolve_values
    )

    return kv_entries
    

def load_brute_force_inner(
    fs_path: str,
    get_store_key: StoreKeyGetter[EntryKey],
    deserialize_key: EntryKeyDeserializer[EntryKey] = identity,
    deserialize_value: EntryValueDeserializer[EntryValue] = identity,
    store_path_dir: StorePath = None,
) -> StoreEntry[EntryKey, EntryValue]:
    if store_path_dir is None:
        store_path_dir = StorePath()

    output: list[EntryKeyValue[EntryKey, EntryValue]] = []

    for file in get_store_dir_files(fs_path, store_path_dir):
        output += load_entries_from_file(
                    fs_path=fs_path,
                    store_path=file,
                    get_store_key=get_store_key,
                    deserialize_key=deserialize_key,
                    deserialize_value=deserialize_value,
                )

    for folder in get_store_dir_folders(fs_path, store_path_dir):
        output += load_brute_force_inner(
            fs_path=fs_path,
            get_store_key=get_store_key,
            deserialize_key=deserialize_key,
            deserialize_value=deserialize_value,
            store_path_dir=folder,
        )

    return output

if __name__ == '__main__':
    main()