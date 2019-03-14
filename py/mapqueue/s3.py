from .base import Key, Map, Optional
import boto3


class S3Map(Map):

    def open(self):
        self._db = boto3.client(
            's3',
            aws_access_key_id=self._config.access,
            aws_secret_access_key=self._config.secret,
            region_name=self._config.region
        )
        if not self._db.get_bucket_location(Bucket=self._config.bucket):
            self._db.create_bucket(Bucket=self._config.bucket)
        return self

    def _put(self, key: Key, value: bytes) -> Key:
        self._db.put_object(
            Bucket=self._config.bucket,
            Key=str(key),
            Body=value
        )
        return key

    def _get(self, key: Key) -> Optional[bytes]:
        return self._db.get(
            Bucket=self._config.bucket,
            Key=str(key)
        )

    def close(self):
        del(self._db)
        return self
