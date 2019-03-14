from .base import namedtuple, Key, Map, Queue, Optional
from bisect import bisect_left
from collections import deque

# inspired by https://code.activestate.com/recipes/577197-sortedcollection/
Revision = namedtuple('Revision', [
    'times',  # negative UTC milliseconds since epoch
    'values'  # stored value
])


class LocalMap(Map):

    def open(self):
        self._map = {}
        return self

    def _get(self, key: Key) -> Optional[bytes]:
        uuid = key.uuid.bytes_le
        if uuid not in self._map.keys():
            return None
        else:
            revisions = self._map[uuid]
            i = bisect_left(revisions.times, -key.time)
            return revisions.values[i]

    def _put(self, key: Key, value: bytes) -> Key:
        uuid = key.uuid.bytes_le
        if uuid not in self._map.keys():
            self._map[uuid] = Revision(
                times=[-key.time],
                values=[value]
            )
        else:
            revisions = self._map[uuid]
            i = bisect_left(revisions.times, -key.time)-1
            revisions.times.insert(i, -key.time)
            revisions.values.insert(i, value)
        return key

    def close(self):
        del(self._map)
        return self


class LocalQueue(Queue):

    def open(self):
        self._queue = deque()
        return self

    def add(self, kind: str, value: bytes) -> bytes:
        self._queue.appendleft(value)
        return value

    def pop(self) -> bytes:
        return self._queue.pop()

    def close(self):
        del(self._queue)
        return self
