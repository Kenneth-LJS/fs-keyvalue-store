from typing import Generic

from .types import EntryKey, EntryValue

class StoreFile(Generic[EntryKey, EntryValue]):
    store_path: str
    write_after: int
    write_count_since_save: int

    def __init__(
        self,
        store_path: str,
        write_after: int = 1
    ):
        self.store_path = store_path
        self.write_after = write_after
        self.write_count_since_save = 0
        
    def open(self):
        pass

    def close(self):
        pass
