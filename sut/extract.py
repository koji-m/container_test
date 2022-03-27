import json
import os
import boto3
from botocore import UNSIGNED
from botocore.client import Config
import sqlalchemy

def get_records_from_rds(table):
    print('start fetch records')
    engine = sqlalchemy.create_engine(os.environ['RDS_CONNECTION_URL'])
    result = engine.execute(
        f'select * from {table}'
    )
    recs = [dict(rec) for rec in result]
    path = f'{table}.json'
    with open(path, 'w') as f:
        f.write(json.dumps(recs))
    print('end fetch records')
    return path

def upload_records_to_s3(path):
    print('start upload file')
    resource = boto3.resource(
        's3',
        endpoint_url=os.environ['S3_ENDPOINT'],
        config=Config(signature_version=UNSIGNED)
    )
    bucket = resource.Bucket(os.environ['S3_BUCKET'])
    with open(path, 'rb') as f:
        bucket.upload_fileobj(f, f.name)
    print('end upload file')

def extract(table):
    path = get_records_from_rds(table)
    upload_records_to_s3(path)

if __name__ == '__main__':
    extract(os.environ['TABLE_NAME'])

