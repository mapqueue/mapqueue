from .base import Key, Map, Optional
from .config import NAME
from sqlite3 import connect

# investigate using WITHOUT ROWID
# https://www.sqlite.org/withoutrowid.html
CREATE = '''CREATE TABLE IF NOT EXISTS {table}(
uuid BLOB NOT NULL,
time INTEGER NOT NULL,
kind TEXT NOT NULL,
value BLOB NOT NULL,
PRIMARY KEY(uuid, kind, time DESC));'''.format(table=NAME)

INSERT = '''INSERT INTO {table}(uuid, kind, time, value)
VALUES (?, ?, ?, ?);'''.format(table=NAME)

SELECT = '''SELECT value from {table}
WHERE uuid = ? AND kind = ? AND time <= ?
ORDER BY time DESC LIMIT 1;'''.format(table=NAME)


class SQLiteMap(Map):

    def open(self):
        self._db = connect(NAME + '.db')
        # prints commands for debugging
        # self._db.set_trace_callback(print)
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

    def _get(self, key: Key) -> Optional[bytes]:
        return self._cursor.execute(
            SELECT,
            (key.uuid.bytes_le, key.kind, key.time)
        ).fetchone()[0]

    def close(self):
        self._cursor.close()
        self._db.close()
        return self
