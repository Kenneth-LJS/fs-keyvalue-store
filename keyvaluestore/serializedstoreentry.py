from typing import TypedDict

from .types import SerializedEntryKey, SerializedEntryValue

class SerializedStoreEntry(TypedDict):
    key: SerializedEntryKey
    timestamp: str
    value: SerializedEntryValue
