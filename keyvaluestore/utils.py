from datetime import datetime, timezone
import math
from collections import defaultdict
from typing import Callable, Iterable, Mapping, TypeVar

from .storeentry import StoreEntry
from .serializedstoreentry import SerializedStoreEntry
from .types import EntryKeySerializer, EntryKeyDeserializer, EntryValueSerializer, EntryValueDeserializer, SerializedEntryKey, SerializedEntryValue

T = TypeVar('T')
U = TypeVar('U')

def serialize_store_entry(
    entry: StoreEntry,
    key_serializer: EntryKeySerializer,
    value_serializer: EntryValueDeserializer,
):
    return {
        'key': key_serializer(entry.key),
        'value': value_serializer(entry.value)
    }


def deserialize_store_entry(
    serialized_entry: SerializedStoreEntry,
    key_deserializer: EntryKeyDeserializer,
    value_deserializer: EntryValueDeserializer,
):
    return StoreEntry(
        key=key_deserializer(serialized_entry['key']),
        # timestamp=serialized_entry['timestamp'],
        value=value_deserializer(serialized_entry['value'])
    )


def identity(x):
    return x


def get_first_value(_, xs: Iterable[T]) -> T:
    return next(iter(xs))


def hex_str_to_bytes(hex_str: str):
    byte_len = math.ceil(len(hex_str) / 2)
    return int(hex_str, 16).to_bytes(byte_len, 'big', False)

# def get_timestamp_str() -> str:
#     return str(datetime.now(timezone.utc))


# def timestamp_str_to_date(timestamp_str: str) -> datetime:
#     return datetime.fromisoformat(timestamp_str)

def group_into_dict(keyfunc: Callable[[T], U], data: list[T]) -> Mapping[U, list[T]]:
    result = defaultdict(list)
    for entry in data:
        key = keyfunc(entry)
        result[key].append(entry)
    return result

def resolve_sorted_iterable_duplicates(
    iter: Iterable[T],
    resolve_duplicates: Callable[[Iterable[T]], T] = get_first_value,
    is_equal: Callable[[T, T], bool] = lambda a, b: a == b,
):
    buffer: list[T] = []
    for item in iter:
        if len(buffer) > 0 and not is_equal(buffer[0], item):
            yield(resolve_duplicates(buffer))
            buffer = []

        buffer.append(item)

    yield(resolve_duplicates(buffer))