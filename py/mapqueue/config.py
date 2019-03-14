from .base import NAME, namedtuple
import boto3
from google.oauth2.service_account import Credentials
import json

# amazon web services
AWS = namedtuple('AWS', [
    'access',  # aws access key
    'bucket',  # s3 bucket
    'dynamodb', # dynamodb client
    'region',  # data center region
    'secret',  # aws secret key
    'table'  # dynamodb table name
])


def aws(access: str = None, secret: str = None,
        bucket: str = NAME, region: str = 'us-east-1', table: str = NAME,
        test: bool = False) -> AWS:
    if test:
        dynamodb = boto3.client(
            'dynamodb',
            endpoint_url='http://localhost:8000'
        )
    else:
        dynamodb = boto3.client(
            'dynamodb',
            aws_access_key_id=access,
            aws_secret_access_key=secret,
            region_name=region
        )
    return AWS(
        access=access,
        bucket=bucket,
        dynamodb=dynamodb,
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
