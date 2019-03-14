from .base import Key, Map, Optional
from .config import NAME
from redis import Redis


class RedisMap(Map):

    def open(self):
        self._db = Redis(db=NAME)
        return self

    def _put(self, key: Key, value: bytes) -> Key:
        self._db.set(key.as_bytes(), value)
        return key

    def _get(self, key: Key) -> Optional[bytes]:
        return self._db.get(key.as_bytes())

    def close(self):
        del(self._db)
        return self
