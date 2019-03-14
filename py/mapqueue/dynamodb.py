import boto3
from .base import Key, Map, Optional


class DynamoMap(Map):

    def open(self):
        self._db = boto3.client(
            'dynamodb',
            aws_access_key_id=self._config.access,
            aws_secret_access_key=self._config.secret,
            region_name=self._config.region
        )
        table_exists = self._db.list_tables(
            ExclusiveStartTableName=self._config.table[:-1],
            Limit=1
        )['TableNames'][0] == self._config.table
        if not table_exists:
            self._db.create_table(
                TableName=self._config.table,
                KeySchema=[
                    {'AttributeName': 'uuid', 'KeyType': 'HASH'},
                    {'AttributeName': 'time', 'KeyType': 'RANGE'}
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'uuid', 'AttributeType': 'B'},
                    {'AttributeName': 'time', 'AttributeType': 'N'},
                    {'AttributeName': 'kind', 'AttributeType': 'S'},
                    {'AttributeName': 'value', 'AttributeType': 'B'}
                ]
            )
        return self

    def _put(self, key: Key, value: bytes) -> Key:
        self._db.put_item(
            TableName=self._config.table,
            Item={
                'uuid': {'B', key.uuid.bytes_le},
                'time': {'N', key.time},
                'kind': {'S', key.kind},
                'value': {'B', value}
            }
        )
        return key

    def _get(self, key: Key) -> Optional[bytes]:
        return self._db.get_item(
            TableName=self._config.table,
            Key={
                'uuid': {'B': key.uuid.bytes_le},
                'time': {'N': key.time}
            },
            ConsistentRead=False
        )

    def close(self):
        del(self._db)
        return self
