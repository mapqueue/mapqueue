import os
from time import sleep
from uuid import uuid4
from zlib import compress
from mapqueue.base import EMPTY, Key, now
from mapqueue.config import gcp
from mapqueue.bigtable import BigTableMap
from mapqueue.local import LocalMap, LocalQueue
from mapqueue.lmdb import LMDBMap
from mapqueue.sqlite import SQLiteMap

TEN = 10000

# bigtable emulator must be running
# docker run -d -p 9035:9035 shopify/bigtable-emulator
os.environ['BIGTABLE_EMULATOR_HOST'] = 'localhost:9035'

def test_map(m):
    NOW = now()
    with m as db:
        hello = db.create(kind='hello', value=b'now')
        db._put(key=Key(kind='hello', uuid=hello.uuid, time=NOW-TEN), value=compress(b'past'))
        db._put(key=Key(kind='hello', uuid=hello.uuid, time=NOW+TEN), value=compress(b'future'))
        world = db.create(kind='world', value=b'world')
        deleted = db.create(kind='deleted', value=b'deleted')
        db.delete(kind=deleted.kind, uuid=deleted.uuid)
        assert db.exists(kind=hello.kind, uuid=hello.uuid, time=NOW)
        assert db.get(key=Key(kind=hello.kind, uuid=hello.uuid, time=NOW)) == b'now'
        assert db.read(kind=hello.kind, uuid=hello.uuid, time=NOW) == b'now'
        assert db.exists(kind=world.kind, uuid=world.uuid, time=NOW)
        assert db.get(key=Key(kind=world.kind, uuid=world.uuid, time=NOW)) == b'world'
        assert db.read(kind=world.kind, uuid=world.uuid, time=NOW) == b'world'
        assert not db.exists(kind=deleted.kind, uuid=deleted.uuid, time=NOW)
        assert db.get(key=Key(kind=deleted.kind, uuid=deleted.uuid, time=NOW)) is None
        assert db.read(kind=deleted.kind, uuid=deleted.uuid, time=NOW) is None
    db = m.open()
    hello = db._put(key=Key(kind='hello', uuid=uuid4(), time=NOW), value=compress(b'now'))
    db._put(key=Key(kind='hello', uuid=hello.uuid, time=NOW-TEN), value=compress(b'past'))
    db._put(key=Key(kind='hello', uuid=hello.uuid, time=NOW+TEN), value=compress(b'future'))
    world = db._put(key=Key(kind='world', uuid=uuid4(), time=NOW), value=compress(b'world'))
    deleted = db._put(key=Key(kind='deleted', uuid=uuid4(), time=NOW-TEN), value=compress(b'deleted'))
    db._put(key=Key(kind='deleted', uuid=deleted.uuid, time=NOW), value=EMPTY)
    assert db.exists(kind=hello.kind, uuid=hello.uuid, time=NOW)
    assert db.get(key=Key(kind=hello.kind, uuid=hello.uuid, time=NOW)) == b'now'
    assert db.read(kind=hello.kind, uuid=hello.uuid, time=NOW) == b'now'
    assert db.exists(kind=world.kind, uuid=world.uuid, time=NOW)
    assert db.get(key=Key(kind=world.kind, uuid=world.uuid, time=NOW)) == b'world'
    assert db.read(kind=world.kind, uuid=world.uuid, time=NOW) == b'world'
    assert not db.exists(kind=deleted.kind, uuid=deleted.uuid, time=NOW)
    assert db.get(key=Key(kind=deleted.kind, uuid=deleted.uuid, time=NOW)) is None
    assert db.read(kind=deleted.kind, uuid=deleted.uuid, time=NOW) is None
    db.close()

def test_queue(q):
    with q as queue:
        expected = sorted([b'bmw', b'volvo'])

        for car in expected:
            queue.add(kind='car', value=car)
        
        actual = sorted([event for event in queue])
        assert len(expected) == len(actual)
        for index, value in enumerate(actual):
            assert value == expected[index]
    queue = q.open()
    expected = sorted([b'mazda', b'toyota'])

    for car in expected:
        queue.add(kind='car', value=car)

    actual = sorted([queue.pop() for car in expected])
    assert len(expected) == len(actual)
    for index, value in enumerate(actual):   
        assert value == expected[index]
    queue.close()

if __name__ == "__main__":
    test_map(BigTableMap(config=gcp(keys='./keys/gcp.json')))
    test_map(LocalMap())
    test_map(LMDBMap())
    test_map(SQLiteMap())
    test_queue(LocalQueue())