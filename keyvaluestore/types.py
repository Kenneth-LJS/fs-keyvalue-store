from dataclasses import dataclass, field
import dataclasses
from typing import Callable, Iterable, NamedTuple, TypeVar, Hashable, Generic, Union
from functools import cached_property
import os
from pathlib import Path

from .constants import ALL_BYTE_HEX_STR, BYTE_HEX_MAP

EntryKey = TypeVar('EntryKey') # immutable, has custom hash, equality check
EntryValue = TypeVar('EntryValue')

@dataclass(repr=True, frozen=True)
class EntryKeyValue(Generic[EntryKey, EntryValue]):
    key: EntryKey
    value: EntryValue

@dataclass(repr=True, frozen=True)
class StoreKey(Generic[EntryKey]):
    key: EntryKey = field(default=None, hash=False)
    hash: int = field(default=None, hash=False)
    hashb: bytes = field(default=None, hash=True) # hashb = hash bytes

@dataclass(repr=True, frozen=True)
class StoreEntry(Generic[EntryKey, EntryValue]):
    key: StoreKey[EntryKey]
    value: EntryValue


# @dataclass(frozen=True)
# class EntryKeyValue(Generic[EntryKey, EntryValue]):
#     key: EntryKey
#     value: EntryValue

# @dataclass(frozen=True)
# class EntryKeyWithByteHash(Generic[EntryKey]):
#     key: EntryKey
#     hex_list: bytes


# EntryKeyWithByteHash = namedtuple('EntryKeyWithByteHash', 'key', 'byte_hash')

EntryKeyHasher = Callable[[EntryKey], int]
EntryKeyByteHasher = Callable[[EntryKey], bytes]

StoreKeyGetter = Callable[[EntryKey], StoreKey[EntryKey]]

EntryValueResolver = Callable[[EntryKey, Iterable[EntryValue]], EntryValue]

EntryKeySerializer = Callable[[EntryKey], str]
EntryKeyDeserializer = Callable[[str], EntryKey]
EntryValueSerializer = Callable[[EntryValue], str]
EntryValueDeserializer = Callable[[str], EntryValue]

SerializedEntryKey = TypeVar('SerializedEntryKey')
SerializedEntryValue = TypeVar('SerializedEntryValue')

@dataclass(frozen=True)
class StorePath:
    parents: bytes = b''
    name: bytes = None # if name is None, then treat this as a folder, otherwise it is the file name

    def __post_init__(self):
        # StorePath.byte_path
        object.__setattr__(self, 'byte_path', self.parents + (self.name or b''))

        # StorePath.path, StorePath.backup_path
        parent_dirs = [ALL_BYTE_HEX_STR[b] for b in self.parents]
        if self.name is None:
            path = os.path.join('', *parent_dirs)
            object.__setattr__(self, 'path', path)
            object.__setattr__(self, 'backup_path', path)
        else:
            if len(self.name) == 0:
                file_name = 'data.json'
            else:
                file_name = f'{"".join(ALL_BYTE_HEX_STR[b] for b in self.name)}.json'

            object.__setattr__(self, 'path', os.path.join('', *parent_dirs, file_name))
            object.__setattr__(self, 'backup_path', os.path.join('', *parent_dirs, f'_{file_name}'))

        # StorePath.is_file, StorePath.is_folder
        object.__setattr__(self, 'is_file', self.name is not None)
        object.__setattr__(self, 'is_folder', self.name is None)

    def join(self, dir_path='', backup=False):
        return os.path.join(dir_path, (self.backup_path if backup else self.path))

    def can_contain(self, obj: Union['StorePath', bytes, StoreKey]):
        if isinstance(obj, StorePath):
            return obj.byte_path.startswith(self.byte_path)
        if isinstance(obj, bytes):
            return obj.startswith(self.byte_path)
        if isinstance(obj, StoreKey):
            return obj.hashb.startswith(self.byte_path)
        raise ValueError(f'{obj} must be bytes, StorePath, or StoreKey')

    def can_be_contained_by(self, obj: Union['StorePath', bytes, StoreKey]):
        if isinstance(obj, StorePath):
            return self.byte_path.startswith(obj.byte_path)
        if isinstance(obj, bytes):
            return self.startswith(obj.byte_path)
        if isinstance(obj, StoreKey):
            return self.byte_path.startswith(obj.hashb)
        raise ValueError(f'{obj} must be bytes, StorePath, or StoreKey')


    def __add__(a: 'StorePath', b: Union['StorePath', bytes]):
        if not a.is_folder:
            raise ValueError(f'{a} is not a StorePath folder - name property should be None')

        if isinstance(b, bytes):
            return a.replace(parents=a.parents + b)
        elif isinstance(b, StorePath):
            return StorePath(
                parents=a.parents + b.parents,
                name=b.name,
            )
        else:
            raise ValueError(f'{b} must be a byte or a StorePath object')

    def __len__(self):
        return len(self.byte_path)

    # def __sub__(a: 'StorePath', b: Union['StorePath', bytes]):
    #     if isinstance(b, StorePath) and b.name is not None:
    #         raise ValueError(f'{b} must be a byte or a StorePath folder object')
    #     if isinstance(b, StorePath):
    #         b_bytes = b.byte_path
    #     elif isinstance(b, bytes):
    #         b_bytes = b
    #     else:
    #         raise ValueError(f'{b} must be a byte or a StorePath folder object')
        
    #     if not a.byte_path.startswith(b):

        

    # def __str__(self):
    #     parents_str = "/".join(ALL_BYTE_HEX_STR[b] for b in self.parents)
    #     if self.name is None:
    #         return f'StorePath(parents="{parents_str}")'
    #     if self.name == b'':
    #         return f'StorePath(parents="{parents_str}", name="data.json")'
    #     name_str = "".join(ALL_BYTE_HEX_STR[b] for b in self.parents)
    #     return f'StorePath(parents="{parents_str}", name={name_str}.json)'

    def replace(self, /, **changes):
        return dataclasses.replace(self, **changes)

    # @classmethod
    # def from_file_path(file_path, fs_path=None):
    #     if fs_path is not None:
    #         file_path = os.path.relpath(file_path, start=fs_path)
    #     path = Path(file_path)
