from collections import namedtuple
from keyvaluestore.constants import ALL_BYTE_HEX_STR
import random
from dataclasses import dataclass
import os

from lib import *

from keyvaluestore.storeutils import *
from keyvaluestore.fileutils import *


# ALL_BYTE_HEX_STR = tuple(f'{i:02X}' for i in range(256))
ALL_BYTES = tuple(bytes([i]) for i in range(256))
BYTE_HEX_MAP = { bytes([i]): f'{i:02X}' for i in range(256) }
HASH_LEN = 32
HASH_BYTE_LEN = HASH_LEN >> 3
MAX_DEPTH = HASH_BYTE_LEN

WORD_KEY_SIZE = 5

JSON_DUMP_ARGS = {
    'indent': 4,
}

def main():
    make_data_1('test_1')
    # make_data_2('test_2')

@dataclass(frozen=True)
class FileFolderDistribution:
    type: str = field(default=None, init=False)

@dataclass(frozen=True)
class FolderDistribution(FileFolderDistribution):
    type: str = field(default='folder', init=False)
    children: list[FileFolderDistribution] = field(default_factory=list)

@dataclass(frozen=True)
class FileDistribution(FileFolderDistribution):
    type: str = field(default='file', init=False)
    depth: int = 0
    key_count: int = 0
    has_original: bool = True
    backup_key_count: int = 0
    has_backup: bool = False


# Simple files store - malformed structure
def make_data_1(store_name):
    def linear_dist(max_depth=MAX_DEPTH, key_count=1, depth=1):
        # depth=1 file
        if depth >= max_depth - 1:
            return FileDistribution(depth=max_depth - depth, key_count=key_count)
        return FolderDistribution(children=[linear_dist(max_depth=max_depth, key_count=key_count, depth=depth + 1)])

    def rand_dist(max_depth=MAX_DEPTH, depth=1):
        if depth >= max_depth - 1:
            return [FileDistribution(depth=max_depth - depth, key_count=random.randint(0, 10))]

        return [
            *[FolderDistribution(children=rand_dist(max_depth=random.randint(depth + 1, max_depth), depth=depth + 1)) for _ in range(random.randint(3, 4))],
            *[FolderDistribution(children=rand_dist(max_depth=max_depth, depth=depth + 1)) for _ in range(1)],
            *[FileDistribution(depth=random.randint(1, max_depth - depth), key_count=random.randint(0, 10)) for _ in range(random.randint(3, 4))],
        ]

    store_distribution = FolderDistribution(
        children=[
            FileDistribution(depth=0, key_count=random.randint(0, 10)),
            *[FolderDistribution(children=rand_dist(max_depth=random.randint(1, 4))) for _ in range(5)],
            *[FolderDistribution(children=rand_dist(max_depth=4)) for _ in range(2)],
            *[linear_dist(max_depth=depth, key_count=1) for depth in range(1, 4)],
            *[linear_dist(max_depth=depth, key_count=5) for depth in range(1, 4)],
            *[linear_dist(max_depth=depth, key_count=1) for depth in range(1, 5)],
            *[linear_dist(max_depth=depth, key_count=5) for depth in range(1, 5)],
        ]
    )
    
    return create_file_store(store_name, store_distribution)

# Dense file store - malformed structure
def make_data_2(store_name):
    def linear_dist(max_depth=MAX_DEPTH, key_count=1, depth=1):
        # depth=1 file
        if depth >= max_depth - 1:
            return FileDistribution(depth=max_depth - depth, key_count=key_count)
        return FolderDistribution(children=[linear_dist(max_depth=max_depth, key_count=key_count, depth=depth + 1)])

    def rand_dist(max_depth=MAX_DEPTH, depth=1):
        if depth >= max_depth - 1:
            return [FileDistribution(depth=max_depth - depth, key_count=random.randint(1000, 10000))]
        folder_count = random.randint(0, 5)
        file_count = random.randint(0, 5)
        file_count_max_depth = 1

        return [
            *[FolderDistribution(children=rand_dist(max_depth=max_depth, depth=depth + 1)) for _ in range(folder_count)],
            *[FolderDistribution(children=rand_dist(max_depth=random.randint(depth + 1, max_depth), depth=depth + 1)) for _ in range(folder_count)],
            *[FileDistribution(depth=random.randint(1, max_depth - depth), key_count=random.randint(1000, 10000)) for _ in range(file_count)],
            *[FileDistribution(depth=max_depth - depth, key_count=random.randint(1000, 10000)) for _ in range(file_count_max_depth)],
        ]

    store_distribution = FolderDistribution(
        children=[
            FileDistribution(depth=0, key_count=random.randint(1000, 10000)),
            *[FileDistribution(depth=depth, key_count=random.randint(1000, 10000)) for depth in range(1, 5) for _ in range(3)],
            *[FolderDistribution(children=rand_dist(max_depth=random.randint(1, MAX_DEPTH + 1))) for _ in range(10)],
            *[linear_dist(max_depth=depth, key_count=random.randint(1000, 10000)) for depth in range(1, 4)],
            *[linear_dist(max_depth=depth, key_count=random.randint(1000, 10000)) for depth in range(1, 4)],
        ]
    )
    
    return create_file_store(store_name, store_distribution)

# Simple files store - malformed structure + backup files
def make_data_3(store_name):
    get_key_count = lambda: random.randint(200, 500)

    def rand_dist(max_depth=MAX_DEPTH, depth=1):
        if depth >= max_depth - 1:
            if bool(random.getrandbits(1)):
                # regular no-backup file
                return [FileDistribution(
                    depth=max_depth - depth,
                    key_count=get_key_count(),
                )]
            else:
                return [FileDistribution(
                    depth=max_depth - depth,
                    key_count=get_key_count(),
                    has_original=bool(random.getrandbits(1)),
                    has_backup=True,
                    backup_key_count=get_key_count(),
                )]

        return [
            *[d for d in rand_dist(max_depth=random.randint(depth + 1, max_depth), depth=depth + 1) for _ in range(random.randint(1, 5))],
            *[d for d in rand_dist(max_depth=max_depth, depth=depth + 1) for _ in range(1)],
            *[
                # Basic file
                FileDistribution(
                    depth=random.randint(1, max_depth - depth),
                    key_count=get_key_count(),
                )
                for _ in range(random.randint(1, 5))
            ],
            *[
                # Basic file
                FileDistribution(
                    depth=max_depth - depth,
                    key_count=random.randint(1000, 10000),
                )
                for _ in range(1)
            ],
            *[
                # Backup only
                FileDistribution(
                    depth=random.randint(1, max_depth - depth),
                    key_count=get_key_count(),
                    has_original=False,
                    has_backup=True,
                    backup_key_count=get_key_count(),
                )
                for _ in range(random.randint(1, 5))
            ],
            *[
                # Backup only
                FileDistribution(
                    depth=max_depth - depth,
                    key_count=get_key_count(),
                    has_original=False,
                    has_backup=True,
                    backup_key_count=get_key_count(),
                )
                for _ in range(1)
            ],
            *[
                # Original and backup
                FileDistribution(
                    depth=random.randint(1, max_depth - depth),
                    key_count=get_key_count(),
                    has_original=True,
                    has_backup=True,
                    backup_key_count=get_key_count(),
                )
                for _ in range(random.randint(1, 5))
            ],
            *[
                # Original and backup
                FileDistribution(
                    depth=max_depth - depth,
                    key_count=get_key_count(),
                    has_original=True,
                    has_backup=True,
                    backup_key_count=get_key_count(),
                )
                for _ in range(1)
            ],
        ]

    store_distribution = FolderDistribution(
        children=[
            FileDistribution(depth=0, key_count=get_key_count()),
            FileDistribution(depth=0, key_count=get_key_count(), has_backup=True, backup_key_count=get_key_count()),
            *[FileDistribution(depth=depth, key_count=get_key_count()) for depth in range(1, 5) for _ in range(4)],
            *[FolderDistribution(children=rand_dist(max_depth=random.randint(1, MAX_DEPTH))) for _ in range(4)],
            FolderDistribution(children=rand_dist(max_depth=MAX_DEPTH))
        ]
    )
    
    return create_file_store(store_name, store_distribution)

# Simple files store - some files have backup
def make_data_4(store_name):
    get_key_count = lambda: random.randint(0, 10)
    def rand_dist(max_depth=MAX_DEPTH, depth=1):
        if depth >= max_depth - 1:
            return [
                FileDistribution(depth=max_depth - depth, key_count=get_key_count()),
                FileDistribution(depth=max_depth - depth, key_count=get_key_count(), has_original=True, has_backup=True, backup_key_count=get_key_count()),
                FileDistribution(depth=max_depth - depth, key_count=get_key_count(), has_original=False, has_backup=True, backup_key_count=get_key_count())
            ]

        return [
            *[FolderDistribution(children=rand_dist(max_depth=random.randint(depth + 1, max_depth), depth=depth + 1)) for _ in range(random.randint(3, 4))],
            *[FolderDistribution(children=rand_dist(max_depth=max_depth, depth=depth + 1)) for _ in range(1)],
            *[FileDistribution(depth=random.randint(1, max_depth - depth), key_count=get_key_count()) for _ in range(random.randint(3, 4))],
            *[FileDistribution(depth=random.randint(1, max_depth - depth), key_count=get_key_count(), has_original=True, has_backup=True, backup_key_count=get_key_count()) for _ in range(random.randint(3, 4))],
            *[FileDistribution(depth=random.randint(1, max_depth - depth), key_count=get_key_count(), has_original=False, has_backup=True, backup_key_count=get_key_count()) for _ in range(random.randint(3, 4))],
        ]

    store_distribution = FolderDistribution(
        children=[
            FileDistribution(depth=0, key_count=get_key_count(), has_backup=True, backup_key_count=get_key_count()),
            *[FileDistribution(depth=depth, key_count=get_key_count()) for depth in range(1, 5) for _ in range(3)],
            *[FileDistribution(depth=depth, key_count=get_key_count(), has_original=True, has_backup=True, backup_key_count=get_key_count()) for depth in range(1, 5) for _ in range(3)],
            *[FileDistribution(depth=depth, key_count=get_key_count(), has_original=False, has_backup=True, backup_key_count=get_key_count()) for depth in range(1, 5) for _ in range(3)],
            *[FolderDistribution(children=rand_dist(max_depth=random.randint(1, 4))) for _ in range(5)],
            *[FolderDistribution(children=rand_dist(max_depth=4)) for _ in range(2)],
        ]
    )
    
    return create_file_store(store_name, store_distribution)


# Simple files store - everything is ONLY backup
def make_data_5(store_name):
    get_key_count = lambda: random.randint(0, 10)
    def rand_dist(max_depth=MAX_DEPTH, depth=1):
        if depth >= max_depth - 1:
            return [FileDistribution(depth=max_depth - depth, key_count=get_key_count(), has_original=False, has_backup=True, backup_key_count=get_key_count())]

        return [
            *[FolderDistribution(children=rand_dist(max_depth=random.randint(depth + 1, max_depth), depth=depth + 1)) for _ in range(random.randint(3, 4))],
            *[FolderDistribution(children=rand_dist(max_depth=max_depth, depth=depth + 1)) for _ in range(1)],
            *[FileDistribution(depth=random.randint(1, max_depth - depth), key_count=get_key_count(), has_original=False, has_backup=True, backup_key_count=get_key_count()) for _ in range(random.randint(3, 4))],
        ]

    store_distribution = FolderDistribution(
        children=[
            FileDistribution(depth=0, key_count=get_key_count(), has_original=False, has_backup=True, backup_key_count=get_key_count()),
            *[FolderDistribution(children=rand_dist(max_depth=random.randint(1, 4))) for _ in range(5)],
            *[FolderDistribution(children=rand_dist(max_depth=4)) for _ in range(2)],
        ]
    )
    
    return create_file_store(store_name, store_distribution)

CreateFileStoreResult = namedtuple(
    'CreateFileStoreResult',
    [
        'valid_entry_count',
        'total_entry_count',
    ]
)

def create_file_store(store_name, store_distribution: FolderDistribution):
    fs_path = get_fs_path(store_name)

    get_store_key = init_get_store_key(hash_word_key, HASH_LEN)
    file_count = 0
    valid_keys = set()
    total_entry_count = 0

    def create_folder_helper(distribution: FolderDistribution, parent_bytes=b''):
        for d in distribution.children:
            if not isinstance(d, FileFolderDistribution):
                print('Attempting to create something that isn\'t a folder:', d)

        child_folders = [child for child in distribution.children if isinstance(child, FolderDistribution)]
        for child_folder, assigned_hex in zip(child_folders, random.sample(ALL_BYTES, len(child_folders))):
            create_folder_helper(child_folder, parent_bytes + assigned_hex)
        
        child_files = [child for child in distribution.children if isinstance(child, FileDistribution) and child.depth > 0]
        if len(child_files) > 0:
            max_child_depth = max(child_file.depth for child_file in child_files)
            for child_depth in range(1, max_child_depth + 1):
                child_files_with_depth = [child_file for child_file in child_files if child_file.depth == child_depth]
                child_files_with_depth = child_files_with_depth[:255 ** child_depth]
                for i, child_file in zip(random.sample(range(255 ** child_depth), k=len(child_files_with_depth)), child_files_with_depth):
                    child_file_bytes = i.to_bytes(child_depth, 'big')
                    create_file_helper(
                        parent_bytes,
                        child_file_bytes,
                        key_count=child_file.key_count,
                        has_original=child_file.has_original,
                        has_backup=child_file.has_backup,
                        backup_key_count=child_file.backup_key_count,
                    )
            
            child_files_no_depth = [child for child in distribution.children if isinstance(child, FileDistribution) and child.depth == 0]
            if len(child_files_no_depth) > 0:
                create_file_helper(
                    parent_bytes,
                    b'', 
                    key_count=child_files_no_depth[0].key_count,
                    has_original=child_files_no_depth[0].has_original,
                    has_backup=child_files_no_depth[0].has_backup,
                    backup_key_count=child_files_no_depth[0].backup_key_count,
                )

    def create_file_helper(parent_bytes, file_bytes, key_count, has_original, has_backup, backup_key_count):
        nonlocal total_entry_count

        store_path = StorePath(parents=parent_bytes, name=file_bytes)

        if has_original:
            kv_entries = create_file_content_helper(
                parent_bytes=parent_bytes,
                file_bytes=file_bytes,
                key_count=key_count
            )
            for entry in kv_entries:
                valid_keys.add(entry.key.key)
            total_entry_count += len(kv_entries)
            save_file(
                store_path=store_path,
                kv_entries=kv_entries,
                is_backup=False,
            )

        if has_backup:
            kv_entries = create_file_content_helper(
                parent_bytes=parent_bytes,
                file_bytes=file_bytes,
                key_count=backup_key_count
            )
            total_entry_count += len(kv_entries)
            if not has_original:
                for entry in kv_entries:
                    valid_keys.add(entry.key.key)
            save_file(
                store_path=store_path,
                kv_entries=kv_entries,
                is_backup=True,
            )

    def create_file_content_helper(parent_bytes, file_bytes, key_count):
        prefix_bytes = parent_bytes + file_bytes
        suffix_key_size = WORD_KEY_SIZE - len(prefix_bytes)
        key_count = min(key_count, 255 ** suffix_key_size)
        
        word_kv_entries = []
        for i in random.sample(range(255 ** suffix_key_size), k=key_count):
            word_key_bytes = i.to_bytes(suffix_key_size, 'big')

            word_key = create_word_key(prefix_bytes + word_key_bytes)
            word_value = compute_word_value(word_key)

            store_key = get_store_key(word_key)
            word_kv_entries.append(StoreEntry(
                key=store_key,
                value=word_value,
            ))

        return word_kv_entries

    def save_file(store_path: StorePath, kv_entries, is_backup):
        nonlocal file_count
        file_count += 1

        serialized_kv_entries = [
            {
                'key': serialize_word_key(entry.key.key),
                'value': serialize_word_value(entry.value)
            }
            for entry in kv_entries
        ]

        file_path = store_path.join(fs_path, backup=is_backup)

        # if file_exists(file_path):
        #     print('====================\n', 'DUPE:', file_path, '\n============================')

        # print(file_count, len(valid_keys), file_path)

        write_json_to_file_direct(
            file_path=file_path,
            data=serialized_kv_entries,
            json_dump_args=JSON_DUMP_ARGS,
        )

    def create_word_key(word_key_bytes=b''):
        word = '-'.join(ALL_BYTE_HEX_STR[b] for b in word_key_bytes)
        return WordKey(
            word,
            f'https://www.google.com/?q={word}'
        )

    create_folder_helper(store_distribution)
    print('Created files:', file_count)
    print('Total created entries:', total_entry_count)
    print('Total valid entries:', len(valid_keys))

    return CreateFileStoreResult(
        total_entry_count=total_entry_count,
        valid_entry_count=len(valid_keys),
    )

def store_exists(store_name):
    return folder_exists(get_fs_path(store_name))

def get_fs_path(store_name):
    return os.path.join('_data', store_name)

if __name__ == '__main__':
    main()