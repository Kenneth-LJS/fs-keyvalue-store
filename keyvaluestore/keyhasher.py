# from typing import Callable, Generic
# import sys

# from .types import EntryKey

# DEFAULT_HASH_SIZE = sys.hash_info.hash_bits

# class KeyHasher(Generic[EntryKey]):
#     hash_func: Callable[[EntryKey], int]
#     hash_byte_func: Callable[[EntryKey], bytes]
#     hash_size: int

#     def __init__(
#         self,
#         hash_func: Callable[[EntryKey], int] = hash,
#         hash_size: int = DEFAULT_HASH_SIZE,
#     ):
#         self.hash_func = hash_func
#         self.hash_size = hash_size
#         self.hash_byte_func = create_hash_byte_func(hash_func, hash_size)

#     def __call__(self, key: EntryKey) -> bytes:
#         return self.hash_byte_func(key)

# def create_hash_byte_func(
#     hash_func: Callable[[EntryKey], int],
#     hash_size: int
# ) -> Callable[[EntryKey], bytes]:

#     if (hash_size & 7) != 0:
#         raise ValueError('hash_size is not a multiple of 8')

#     hash_size_minus_1 = hash_size - 1
#     hash_size_byte_len = hash_size >> 3

#     add_diff = 1 << hash_size_minus_1

#     def hash_key_bytes(key: EntryKey) -> EntryKeyWithByteHash[EntryKey]:
#         return (hash_func(key) + add_diff).to_bytes(hash_size_byte_len, 'big', signed=False)

#     return hash_key_bytes