# from typing import Callable, Generic, Optional
# import json
# import os
# import sys


# # from .keyhasher import KeyHasher, DEFAULT_HASH_SIZE, create_hash_byte_func
# from .types import EntryKey, EntryValue, EntryKeyHasher, EntryKeyByteHasher, EntryKeySerializer, EntryKeyDeserializer, EntryValueSerializer, EntryValueDeserializer, StoreKeyGetter
# from .storeaccessor import StoreAccessor
# from .storeentry import StoreEntry, StoreKey
# from .utils import deserialize_store_entry, serialize_store_entry, identity
# from .fileutils import mkdir_for_path
# from .constants import DEFAULT_HASH_LEN

# def StoreIterator(
#     store_path: str,
#     get_store_key: StoreKeyGetter[EntryKey],
#     key_hash_len: int = DEFAULT_HASH_LEN,
#     deserialize_key: EntryKeyDeserializer = identity,
#     deserialize_value: EntryValueDeserializer = identity,
# ):

#     def InnerStoreIterator():

#         pass
