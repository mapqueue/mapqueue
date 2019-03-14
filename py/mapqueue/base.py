# Interfaces for Map and Queue
from collections import namedtuple
from datetime import datetime, timezone
from typing import Optional
from uuid import uuid4, UUID
from zlib import compress, decompress

ENDIAN = 'little'
EMPTY = b''  # encoding to represent None
NAME = 'mapqueue'
MILLIS = 1000.0
SIGNED = True
UTF8 = 'utf-8'

# key to unique identify values in a Map
Key = namedtuple('Key', [
    'uuid',
    'time',  # milliseconds since the unix epoch
    'kind'
])


def bytes_key(b: bytes) -> Key:
    """convert Key to bytes"""
    return Key(
        uuid=UUID(bytes_le=b[:16]),
        time=-bytes_int(b[16:24]),
        kind=b[24:].decode(UTF8)
    )


def bytes_int(b: bytes) -> int:
    """convert bytes to int"""
    return int.from_bytes(bytes=b, byteorder=ENDIAN, signed=SIGNED)


def dt_millis(dt: datetime) -> int:
    """convert datetime to milliseconds since the unix epoch"""
    return round(dt.timestamp() * MILLIS)


def key_bytes(key: Key) -> bytes:
    """"convert Key to bytes with the format uid, -time, key"""
    return key.uuid.bytes_le + int_bytes(-key.time) + key.kind.encode(UTF8)


def key_str(key: Key) -> str:
    """"convert the Key to string with the format uid, -time, key"""
    return ''.join([key.uuid.hex, str(-key.time), key.kind])


def int_bytes(i: int) -> bytes:
    """convert int to bytes"""
    return i.to_bytes(length=8, byteorder=ENDIAN, signed=SIGNED)


def is_none(value: bytes) -> bool:
    """determine if the value is empty or None"""
    return value is None or value == EMPTY


def millis_dt(millis: int) -> datetime:
    """convert milliseconds since the unix epoch to datetime"""
    return datetime.fromtimestamp(millis / MILLIS)


def now() -> int:
    """return current time as milliseconds since the unix epoch"""
    return dt_millis(datetime.now(timezone.utc))


def str_key(s: str) -> Key:
    """convert string to Key"""
    return Key(uuid=s[:32], time=s[32:45], kind=s[45:])


class Context(object):
    """base class for append only map or queue"""

    def open(self):
        """method to open the map or queue for reads and writes"""
        return self

    def close(self):
        """method to close the map or queue and clean up resources"""
        return self

    def __init__(self, config=None):
        """method to initialize the python object and store configuration"""
        self._config = config

    def __enter__(self):
        """creates relevant resources using python with statement"""
        return self.open()

    def __exit__(self, exit_type, exit_value, traceback):
        """cleans up relevant resources using python with statement"""
        return self.close()


class Map(Context):
    """key/value storage where put appends values as new versions and get retrieves the latest value"""

    def _get(self, key: Key) -> Optional[bytes]:
        """get returns the latest value that was valid at the time specified otherwise None"""
        raise NotImplementedError(
            'get must return the latest value that was valid at the time specified otherwise None')

    def _put(self, key: Key, value: bytes) -> Key:
        """put stores value into map for given kind, uuid and time"""
        raise NotImplementedError(
            'put must store value into map for given kind, uuid and time')

    def get(self, key: Key) -> Optional[bytes]:
        """get returns the latest value that was valid at the time specified otherwise None"""
        value = self._get(key=key)
        return None if is_none(value) else decompress(value)

    def put(self, kind: str, uuid: UUID, value: bytes) -> Key:
        """put stores value into map for given kind, uuid and time"""
        return self._put(
            key=Key(kind=kind, uuid=uuid4(), time=now()),
            value=EMPTY if is_none(value) else compress(value)
        )

    def create(self, kind: str, value: bytes) -> Key:
        """create a new uuid for the kind and value"""
        return self.put(kind=kind, uuid=uuid4(), value=value)

    def delete(self, kind: str, uuid: UUID) -> Key:
        """delete the key but adding an empty byte value. all other values of the key are retained."""
        return self.put(kind=kind, uuid=uuid, value=EMPTY)

    def exists(self, kind: str, uuid: UUID, time: int = now()) -> bool:
        """exists returns True if the key has a value besides None or empty bytes in the Map"""
        return not is_none(self.get(key=Key(kind=kind, uuid=uuid, time=time)))

    def read(self, kind: str, uuid: UUID, time: int = now()) -> Optional[bytes]:
        """read returns the latest value that was valid at the time specified otherwise None"""
        return self.get(key=Key(kind=kind, uuid=uuid, time=time))

    def update(self, kind: str, uuid: UUID, value: bytes) -> Key:
        """update stores value into map for given kind and uuid"""
        return self.put(kind=kind, uuid=uuid, value=value)


class Queue(Context):
    """queue with no ordering guarantees - values are appended to the end but may pop off out of order"""

    def add(self, kind: str, value: bytes) -> bytes:
        """adds value to queue"""
        raise NotImplementedError('add must append a value to the queue')

    def pop(self) -> bytes:
        raise NotImplementedError('pop must return a value from the queue')

    def __iter__(self):
        """implement python iterator"""
        return self

    def __next__(self):
        """implement for loop syntax"""
        try:
            return self.pop()
        except Exception as e:
            raise StopIteration(str(e))
