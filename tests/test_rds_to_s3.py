import json
import docker
import sqlalchemy

def test_rds_to_s3(s3_and_mysql):
    s3, mysql = s3_and_mysql
    table_name = 'kimetsu'
    col_name = 'name'
    col_kokyu = 'kokyu'
    client = docker.from_env()
    expected = [
        {col_name: 'tanjiro', col_kokyu: 'mizu'},
        {col_name: 'zenitsu', col_kokyu: 'kaminari'},
        {col_name: 'inosuke', col_kokyu: 'kedamono'},
    ]
    engine = sqlalchemy.create_engine(mysql.get_connection_url())
    engine.execute(f'CREATE TABLE {table_name} ({col_name} VARCHAR(50), {col_kokyu} VARCHAR(50));')
    for rec in expected:
        engine.execute(f"INSERT INTO {table_name} ({col_name}, {col_kokyu}) VALUES ('{rec[col_name]}', '{rec[col_kokyu]}');")

    client.containers.run(
        'rds-to-s3',
        auto_remove=True,
        environment={
            'S3_ENDPOINT': s3.get_http_endpoint_for_docker_network(),
            'S3_BUCKET': s3.get_initial_bucket(),
            'RDS_CONNECTION_URL': mysql.get_connection_url_for_docker_network(),
            'TABLE_NAME': table_name,
        },
        network='mysql_nw',
    )

    f = s3.get_fileobj(
        key=f'{table_name}.json'
    )
    actual = json.load(f)
    f.close()

    assert actual == expected