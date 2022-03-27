import boto3
from botocore import UNSIGNED
from botocore.client import Config
import testcontainers.mysql
from testcontainers.core.container import DockerContainer

class MySqlContainer(testcontainers.mysql.MySqlContainer):
    def __init__(self, image, **kwargs):
        if image:
            super().__init__(image, **kwargs)
        else:
            super().__init__(**kwargs)

    def get_connection_url_for_docker_network(self):
        if self._container is None:
            raise RuntimeError('container has not been started')
        dialect = 'mysql+pymysql'
        username = self.MYSQL_USER
        password = self.MYSQL_PASSWORD
        host = self._name
        port = self.port_to_expose
        db_name = self.MYSQL_DATABASE
        url = f'{dialect}://{username}:{password}@{host}:{port}/{db_name}'

        return url


class S3MockContainer(DockerContainer):
    def __init__(self, image='adobe/s3mock:latest', initial_bucket='test', root='/tmp/buckets', http_port=9090, https_port=9191, **kwargs):
        super().__init__(image, **kwargs)
        self.with_bind_ports(http_port, http_port)
        self.with_bind_ports(https_port, https_port)
        self.with_env('initialBuckets', initial_bucket)
        self.with_env('root', root)
        self.initial_bucket = initial_bucket
        self.root = root
        self.http_port = http_port
        self.https_port = https_port

    def get_initial_bucket(self):
        return self.initial_bucket

    def get_http_endpoint(self):
        return f'http://localhost:{self.http_port}'

    def get_https_endpoint(self):
        return f'http://localhost:{self.https_port}'

    def get_http_endpoint_for_docker_network(self):
        return f'http://{self._name}:{self.http_port}'

    def get_https_endpoint_for_docker_network(self):
        return f'http://{self._name}:{self.https_port}'

    def get_fileobj(self, key):
        client = boto3.client(
            's3',
            endpoint_url=self.get_http_endpoint(),
            config=Config(signature_version=UNSIGNED)
        )
        print(client.list_objects(Bucket=self.initial_bucket))
        with open(key, 'wb') as wf:
            client.download_fileobj(self.initial_bucket, key, wf)

        rf = open(key, 'r')
        return rf
