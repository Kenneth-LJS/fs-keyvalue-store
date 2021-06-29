from typing import Any
from dataclasses import dataclass, field
import hashlib
from datetime import datetime, timezone
import random

from keyvaluestore.storeutils.storefileutil import StoreFileUtil

ENTRY_KEY_HASH_ALGO = 'sha512_256'
HASH_WORD_KEY_LEN = 32 # must be multiple of 8
HASH_WORD_KEY_BYTE_LEN = HASH_WORD_KEY_LEN >> 3
HASH_WORD_KEY_BYTE_LEN_DOUBLE = HASH_WORD_KEY_BYTE_LEN << 1

KEY_WORD_BIT_LEN = HASH_WORD_KEY_LEN + 8 # add more bits at the end, must be multiple of 8
KEY_WORD_BYTE_LEN = KEY_WORD_BIT_LEN >> 3
KEY_WORD_BYTE_LEN_DOUBLE = KEY_WORD_BYTE_LEN << 1

def get_timestamp_str() -> str:
    return str(datetime.now(timezone.utc))

@dataclass(repr=True, eq=True, frozen=True)
class WordKey:
    word: str
    source: str

    def __str__(self):
        return f'<WordKey "{self.word}">'

def hash_word_key(word_key: WordKey):
    h = hashlib.new(ENTRY_KEY_HASH_ALGO, usedforsecurity=False)
    h.update(word_key.word.encode())
    h.update('###'.encode()) # separator
    h.update(word_key.source.encode())
    byte_digest = h.digest()
    byte_digest_trimmed = byte_digest[:4]
    # print('x', len(byte_digest_trimmed), int.from_bytes(byte_digest_trimmed, byteorder='big', signed=True).bit_length())
    return int.from_bytes(byte_digest_trimmed, byteorder='big', signed=True)

# SIMPLE!
key_shift_amt = 1 << (HASH_WORD_KEY_LEN - 1)
def hash_word_key(word_key: WordKey):
    return int(word_key.word.replace('-', '')[:HASH_WORD_KEY_BYTE_LEN_DOUBLE], 16) - key_shift_amt

def serialize_word_key(word_key: WordKey):
    return {
        'word': word_key.word,
        'source': word_key.source
    }

def deserialize_word_key(serialized_word_key: dict[str, Any]):
    return WordKey(**serialized_word_key)

@dataclass(repr=True, eq=True, frozen=True)
class WordValue:
    word: str
    source: str
    timestamp_str: str = field(default_factory=get_timestamp_str, repr=False)
    entries: list[str] = None

def serialize_word_value(word_value: WordValue):
    return {
        'word': word_value.word,
        'source': word_value.source,
        'timestamp_str': word_value.timestamp_str,
        'entries': word_value.entries,
    }

def deserialize_word_value(serialized_word_value: dict[str, Any]):
    return WordValue(**serialized_word_value)

def get_word_keys():
    # all_word_chars = '0123456789ABCDEF'
    # word_char_len = 4
    # # all_word_chars = '9876543210'
    # # word_char_len = 1
    # for i, word_chars in enumerate(itertools.product(all_word_chars, repeat=word_char_len)):
    #     # if i > 7:
    #         # return

    #     word = ''.join(word_chars)
    #     yield WordKey(
    #         word,
    #         f'https://www.google.com/?q={word}'
    #     )

    for i in range(0, 2 ** (HASH_WORD_KEY_LEN + 8)):
        # +1 more byte

        if i > 7:
            return

        yield create_word_from_index(i)

def generate_random_word_key():
    return create_word_from_index(random.randint(0, 2 ** KEY_WORD_BIT_LEN))

def create_word_from_index(i):
    hex_str = f'{i:0{KEY_WORD_BYTE_LEN_DOUBLE}X}' # to hex str
    word = '-'.join([hex_str[j:j+2] for j in range(0, len(hex_str), 2)])
    return WordKey(
        word,
        f'https://www.google.com/?q={word}'
    )

def compute_word_value(word_key: WordKey) -> WordValue:
    val_count = 1 + (hash_word_key(word_key) % 5) # seeded rand value

    word = word_key.word
    word_entries = [f'{word}_{i + 1}' for i in range(val_count)]

    return WordValue(
        word=word_key.word,
        source=word_key.source,
        entries=word_entries
    )


def resolve_word_values(key: WordKey, values: list[WordValue]) -> WordValue:
    print(key, values)
    return max(values, key=lambda value: value.timestamp_str)


def get_word_store_util(fs_path) -> StoreFileUtil[WordKey, WordValue]:
    return StoreFileUtil(
        fs_path=fs_path,
        key_hash_func=hash_word_key,
        key_hash_len=HASH_WORD_KEY_LEN,
        resolve_values=resolve_word_values,
        serialize_key=serialize_word_key,
        deserialize_key=deserialize_word_key,
        serialize_value=serialize_word_value,
        deserialize_value=deserialize_word_value,
        json_dump_args={
            'indent': 4
        },
    )