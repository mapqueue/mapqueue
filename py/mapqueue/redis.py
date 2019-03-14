from .base import Key, Map, int_bytes, Optional, UUID
from .config import NAME
from redis import Redis as connect


class RedisMap(Map):

    def open(self):
        self._db = connect(db=NAME)
        return self

    def _put(self, key: Key, value: bytes) -> Key:
        self._db.set(key.uuid.bytes_le + int_bytes(-key.time), value)
        return key

    def _get(self, uuid: UUID, time: int) -> Optional[bytes]:
        return self._db.get(uuid.bytes_le + int_bytes(-time))

    def close(self):
        del(self._db)
        return self
