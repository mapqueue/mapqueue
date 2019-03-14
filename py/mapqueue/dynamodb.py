import boto3
from .base import Key, is_none, Map, Optional, UUID


class DynamoMap(Map):

    def open(self):
        self._db = self._config.dynamodb
        table_query = self._db.list_tables(
            ExclusiveStartTableName=self._config.table[:-1],
            Limit=1
        )['TableNames']
        table_exists = len(
            table_query) == 1 and table_query[0] == self._config.table
        if not table_exists:
            self._db.create_table(
                TableName=self._config.table,
                KeySchema=[
                    {'AttributeName': 'uuid', 'KeyType': 'HASH'},
                    {'AttributeName': 'time', 'KeyType': 'RANGE'}
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'uuid', 'AttributeType': 'B'},
                    {'AttributeName': 'time', 'AttributeType': 'N'}
                ],
                BillingMode='PAY_PER_REQUEST'
            )
        return self

    def _put(self, key: Key, value: bytes) -> Key:
        item = {
            'uuid': {'B': key.uuid.bytes_le},
            'time': {'N': str(key.time)},
            'kind': {'S': key.kind}
        }
        if not is_none(value):
            item['value'] = {'B': value}
        self._db.put_item(
            TableName=self._config.table,
            Item=item
        )
        return key

    def _get(self, uuid: UUID, time: int) -> Optional[bytes]:
        value = self._db.get_item(
            TableName=self._config.table,
            Key={
                'uuid': {'B': uuid.bytes_le},
                'time': {'N': str(time)}
            },
            ConsistentRead=False
        )['Item']
        return value['value']['B'] if 'value' in value.keys() else None

    def close(self):
        del(self._db)
        return self
