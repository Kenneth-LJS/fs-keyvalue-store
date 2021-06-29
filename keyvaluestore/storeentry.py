from dataclasses import dataclass
from typing import Generic

from .types import EntryKey, EntryValue

@dataclass(repr=True, frozen=True)
class StoreKey(Generic[EntryKey]):
    key: EntryKey
    hash: int
    hashb: bytes # hashb = hash bytes

@dataclass(repr=True, frozen=True)
class StoreEntry(Generic[EntryKey, EntryValue]):
    key: EntryKey
    value: EntryValue