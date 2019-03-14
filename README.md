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

    def get(self, key: Key) -> Optional[bytes]:
        """get returns the latest value that was valid at the time specified otherwise None"""

    def put(self, kind: str, uuid: UUID, value: bytes) -> Key:
        """put stores value into map for given kind, uuid and time"""

    def create(self, kind: str, value: bytes) -> Key:
        """create a new uuid for the kind and value"""
        return self.put(kind=kind, uuid=uuid4(), value=value)

    def delete(self, kind: str, uuid: UUID) -> Key:
        """delete the key but adding an empty byte value. all other values of the key are retained."""
        return self.put(kind=kind, uuid=uuid, value=EMPTY)

    def exists(self, kind: str, uuid: UUID, time: int = now()) -> bool:
        """exists returns True if the key has a value besides None or empty bytes in the Map"""
        return self.get(key=Key(kind=kind, uuid=uuid, time=time)) is not None

    def read(self, kind: str, uuid: UUID, time: int = now()) -> Optional[bytes]:
        """read returns the latest value that was valid at the time specified otherwise None"""
        return self.get(key=Key(kind=kind, uuid=uuid, time=time))

    def update(self, kind: str, uuid: UUID, value: bytes) -> Key:
        """update stores value into map for given kind and uuid"""
        return self.put(kind=kind, uuid=uuid, value=value)
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

