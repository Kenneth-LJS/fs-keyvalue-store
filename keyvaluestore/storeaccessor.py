from __future__ import annotations # for type checking

from typing import Generic, TYPE_CHECKING

from .types import EntryKey, EntryValue

if TYPE_CHECKING:
    from .storemanager import StoreManager

# class StoreAccessor(dict, Generic[EntryKey, EntryValue]):
#     _dict: dict[EntryKey, EntryValue]

#     def __init__(self):
#         self._dict = {}

#     def __setitem__(self, key: EntryKey, item: EntryValue):
#         self._dict[key] = item

#     def __getitem__(self, key: EntryKey) -> EntryValue:
#         return self._dict[key]

#     def __repr__(self):
#         return repr(self._dict)

#     def __len__(self):
#         return len(self._dict)

#     def __delitem__(self, key):
#         del self._dict[key]

#     def clear(self):
#         return self._dict.clear()

#     def copy(self):
#         return self._dict.copy()

#     def update(self, *args, **kwargs):
#         return self._dict.update(*args, **kwargs)

#     def keys(self):
#         return self._dict.keys()

#     def values(self):
#         return self._dict.values()

#     def items(self):
#         return self._dict.items()

#     def pop(self, *args):
#         return self._dict.pop(*args)

#     def __cmp__(self, dict):
#         return self._dict.__cmp__(dict)

#     def __contains__(self, item):
#         return item in self._dict

#     def __iter__(self):
#         return iter(self._dict)

# class StoreAccessor(Generic[EntryKey, EntryValue]):

#     def __init__(self, store_manager):
#         self.store_manager = store_manager

#     def has_key(self, key: EntryValue) -> bool:
#         return self.store_manager.has_key(key)

#     def get_value(self, key: EntryKey) -> EntryValue:
#         return self.store_manager.get_value(key)

#     def set_value(self, key: EntryKey, data: EntryValue):
#         return self.store_manager.set_value(key, data)

#     def remove_key(self, key: EntryKey):
#         return self.store_manager.remove_key(key)

class StoreAccessor(dict, Generic[EntryKey, EntryValue]):
    store_manager: StoreManager[EntryKey, EntryValue]

    def __init__(self, store_manager: StoreManager[EntryKey, EntryValue]):
        self._dict = {}
        self.store_manager = store_manager

    def __setitem__(self, key: EntryKey, value: EntryValue):
        self.store_manager.set_value(key, value)

    def __getitem__(self, key: EntryKey) -> EntryValue:
        return self.store_manager.get_value(key)

    def __repr__(self):
        return super(self).__repr__(self)
        # return repr(self._dict)

    def __len__(self):
        raise NotImplementedError()
        # return len(self._dict)

    def __delitem__(self, key: EntryKey):
        self.store_manager.remove_key(key)

    def clear(self):
        raise NotImplementedError()
        # return self._dict.clear()

    def copy(self):
        raise NotImplementedError()
        # return self._dict.copy()

    def update(self, *args, **kwargs):
        raise NotImplementedError()
        # return self._dict.update(*args, **kwargs)

    def keys(self):
        raise NotImplementedError()
        # return self._dict.keys()

    def values(self):
        raise NotImplementedError()
        # return self._dict.values()

    def items(self):
        raise NotImplementedError()
        # return self._dict.items()

    def pop(self, *args):
        raise NotImplementedError()
        # return self._dict.pop(*args)

    def popitem(self, *args):
        raise NotImplementedError()
        # return self._dict.popitem(*args)

    # def __cmp__(self, dict):
    #     return self._dict.__cmp__(dict)

    def __contains__(self, key: EntryKey):
        return self.store_manager.has_key(key)

    def __iter__(self):
        raise NotImplementedError()
        return iter(self._dict)

    def flush(self):
        self.store_manager.flush()