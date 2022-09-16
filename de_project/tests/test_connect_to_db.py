import pytest
from sqlalchemy import create_engine


host = 'pgdatabase'
user = 'user'
password = 'password1'
port = 5432
db = 'policedb'


def test_engine():
    assert create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')


@pytest.fixture()
def engine():
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    return engine

def test_connection(engine):
    assert engine.connect

@pytest.mark.xfail
def test_wrong_password():
    password = 'p1'
    assert create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')




