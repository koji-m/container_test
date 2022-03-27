import pytest
from containers import MySqlContainer, S3MockContainer

@pytest.fixture()
def s3_container():
    return S3MockContainer().with_name('s3').with_kwargs(network='mysql_nw')

@pytest.fixture()
def mysql_container():
    return MySqlContainer('mysql:5.7.17').with_name('mysql').with_kwargs(network='mysql_nw')

@pytest.fixture()
def s3_and_mysql(s3_container, mysql_container):
    s3_container.start()
    mysql_container.start()

    yield (s3_container, mysql_container)

    s3_container.stop()
    mysql_container.stop()
