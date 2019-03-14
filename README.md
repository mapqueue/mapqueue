MapQueue
========

Standard API to store and read data using [Map](https://en.wikipedia.org/wiki/Associative_array) and [Queue](https://en.wikipedia.org/wiki/Queue_(abstract_data_type)). 

#### Goals
1. Same API across many databases - BigTable, DynamoDB, MySQL, PostgreSQL, Redis, etc
2. Same API across many languages - Python, Go, Java, etc
3. Easy to use with any serialization format - values are bytes

#### Map API

```python
class Key:
    uuid: UUID
    time: int # milliseconds since the unix epoch
    kind: str 
```

```python
class Map:
    """key/value storage where put appends values as new versions and get retrieves the latest value"""

    def _get(self, uuid: UUID, time: int) -> Optional[bytes]:
        """_get retrieves the compressed value for the key"""
        raise NotImplementedError(
            '_get retrieves the compressed value for the key')

    def _put(self, key: Key, value: bytes) -> Key:
        """_put stores the compressed value for the key"""
        raise NotImplementedError(
            '_put stores the compressed value for the key')

    def exists(self, uuid: UUID, time: Optional[int] = None) -> bool:
        """exists returns True if the key has a value besides None or empty bytes in the Map"""
        return self.read(uuid=uuid, time=get_or_now(time)) is not None

    def create(self, kind: str, value: bytes) -> Key:
        """create a new uuid for the kind and value"""
        return self.update(uuid=uuid4(), kind=kind, value=value)

    def read(self, uuid: UUID, time: Optional[int] = None) -> Optional[bytes]:
        """read returns the latest value that was valid at the time specified otherwise None"""
        value = self._get(uuid=uuid, time=get_or_now(time))
        return None if is_none(value) else decompress(value)

    def update(self, uuid: UUID, kind: str, value: bytes) -> Key:
        """update stores value into map for given kind, uuid and time"""
        return self._put(
            key=Key(kind=kind, uuid=uuid, time=now()),
            value=EMPTY if is_none(value) else compress(value)
        )

    def delete(self, uuid: UUID) -> Key:
        """delete the key but adding an empty byte kind and value"""
        return self.update(uuid=uuid, kind=EMPTY, value=EMPTY)
```

#### Queue API

```python
class Queue:
    """queue with no ordering guarantees - values are appended to the end but may pop off out of order"""

    def add(self, kind: str, value: bytes) -> bytes:
        """adds value to queue"""

    def pop(self) -> bytes:
        """returns a value from the queue"""
```

