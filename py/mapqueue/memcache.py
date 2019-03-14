import lmdb
from .base import Key, key_bytes, Map, Optional, UUID
from pymemcache.client.hash import HashClient


class MemcacheMap(Map):

    def open(self):
        self._db = HashClient(('127.0.0.1', 11211))
        return self

    def _put(self, key: Key, value: bytes) -> Key:
        self._db.set(key_bytes(key), value)
        return key

    def _get(self, uuid: UUID, time: int) -> Optional[bytes]:
        return self._db.get(key_bytes(key))

    def close(self):
        del(self._db)
        return self
