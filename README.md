MapQueue
========

Standard API to store and read data using [Map](https://en.wikipedia.org/wiki/Associative_array) and [Queue](https://en.wikipedia.org/wiki/Queue_(abstract_data_type)). Both data structures support `add` for appending data. `Map` supports `get` to read data by key. `Queue` supports `pop` to read and destroy the first value `add`ed.

#### Goals
1. Same API across many databases - BigTable, DynamoDB, MySQL, PostgreSQL, Redis, etc
2. Same API across many languages - Python, Go, Java, etc
3. Easy to use with any serialization format - Avro, Protobuf, etc