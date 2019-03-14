import lmdb
from .base import Key, int_bytes, key_bytes, Map, Optional, UUID

PATH = './'


class LMDBMap(Map):

    def open(self):
        self._env = lmdb.open(path=PATH)
        self._txn = self._env.begin(write=True)
        self._cursor = self._txn.cursor()
        return self

    def _put(self, key: Key, value: bytes) -> Key:
        self._cursor.put(
            key=key_bytes(key),
            value=value
        )
        return key

    def _get(self, uuid: UUID, time: int) -> Optional[bytes]:
        self._cursor.set_range(uuid.bytes_le + int_bytes(-time))
        return self._cursor.value()

    def close(self):
        self._cursor.close()
        self._txn.commit()
        self._env.close()
        return self
