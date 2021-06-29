import sys

DEFAULT_HASH_LEN = sys.hash_info.hash_bits
ALL_BYTE_HEX_STR = tuple(f'{i:02X}' for i in range(256)) # lookup from int

BYTE_MAP = tuple(bytes([i]) for i in range(256))
BYTE_HEX_MAP = { bytes([i]): f'{i:02X}' for i in range(256) }
BYTE_INT_MAP = { bytes([i]): i for i in range(256) }
REVERSE_BYTE_HEX_MAP = { f'{i:02X}': bytes([i]) for i in range(256) }