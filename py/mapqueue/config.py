from .base import NAME, namedtuple
from google.oauth2.service_account import Credentials
import json

# amazon web services
AWS = namedtuple('AWS', [
    'access',  # aws access key
    'bucket',  # s3 bucket
    'region',  # data center region
    'secret',  # aws secret key
    'table'  # dynamodb table name
])


def aws(access: str, secret: str, bucket: str = NAME, region: str = 'us-east-1', table: str = NAME) -> AWS:
    return AWS(
        access=access,
        bucket=bucket,
        region=region,
        secret=secret,
        table=table
    )


# google cloud platform
GCP = namedtuple('GCP', [
    'bucket',  # cloud storage bucket
    'credentials',  # credentials
    'instance',  # bigtable instance
    'keys',  # filename for the service account json
    'project',  # project
    'table'  # bigtable table
])


def gcp(keys: str, bucket: str = NAME, instance: str = NAME, table: str = NAME) -> GCP:
    with open(keys) as f:
        account = json.loads(f.read())
        return GCP(
            bucket=bucket,
            credentials=Credentials.from_service_account_file(keys),
            instance=instance,
            keys=keys,
            project=account['project_id'],
            table=table
        )
