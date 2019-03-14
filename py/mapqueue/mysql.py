from .base import Key, Map, Optional, UUID
from .config import NAME
from MySQLdb import connect

CREATE = '''CREATE TABLE IF NOT EXISTS {table}(
uuid BINARY(16) NOT NULL,
time BIGINT NOT NULL,
kind VARCHAR(255) NOT NULL,
value BLOB NOT NULL,
PRIMARY KEY(uuid, time DESC));'''.format(table=NAME)

INSERT = '''INSERT INTO {table}(uuid, kind, time, value)
VALUES (?, ?, ?, ?);'''.format(table=NAME)

SELECT = '''SELECT value from {table}
WHERE uuid = ? AND time <= ?
ORDER BY time DESC LIMIT 1;'''.format(table=NAME)


class MySQLMap(Map):

    def open(self):
        self._db = connect(db=NAME, user='root')
        self._cursor = self._db.cursor()
        self._cursor.execute(CREATE)
        self._db.commit()
        return self

    def _put(self, key: Key, value: bytes) -> Key:
        self._cursor.execute(
            INSERT,
            (key.uuid.bytes_le, key.kind, key.time, value)
        )
        self._db.commit()
        return key

    def _get(self, uuid: UUID, time: int) -> Optional[bytes]:
        return self._cursor.execute(
            SELECT,
            (uuid.bytes_le, time)
        ).fetchone()[0]

    def close(self):
        self._cursor.close()
        self._db.close()
        return self
