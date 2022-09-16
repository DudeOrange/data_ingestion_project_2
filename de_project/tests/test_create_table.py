import pytest
from sqlalchemy import create_engine, Table, Column, Integer, String, DateTime, Time, Float, Boolean, BigInteger, SMALLINT, MetaData


host = 'localhost'
user = 'user'
password = 'password1'
port = 5432
db = 'policedb'


@pytest.fixture()
def engine():
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    
    return engine


def test_create_table(engine):
    engine.connect
    meta = MetaData()

    temp_table = Table(
        'temp_table', meta,
        Column('INCIDENT_DATETIME', DateTime, nullable=False), 
        Column('INCIDENT_DATE', DateTime, nullable=False), 
        Column('INCIDENT_TIME', Time, nullable=False), 
        Column('INCIDENT_YEAR', SMALLINT, nullable=False), 
        Column('INCIDENT_DAY_OF_WEEK', String(9), nullable=False), 
        Column('REPORT_DATETIME', DateTime, nullable=False), 
        Column('ROW_ID', BigInteger, nullable=False, primary_key=True), 
        Column('INCIDENT_ID', Integer, nullable=False), 
        Column('INCIDENT_NUMBER', Integer, nullable=False), 
        Column('CAD_NUMBER', Float), 
        Column('REPORT_TYPE_CODE', String(2), nullable=False), 
        Column('REPORT_TYPE_DESCRIPTION', String(19), nullable=False), 
        Column('FILED_ONLINE', Boolean), 
        Column('INCIDENT_CODE', Integer), 
        Column('INCIDENT_CATEGORY', String(44)), 
        Column('INCIDENT_SUBCATEGORY', String(40)), 
        Column('INCIDENT_DESCRIPTION', String(84)), 
        Column('RESOLUTION', String(20)), 
        Column('INTERSECTION', String(84)), 
        Column('CNN', Float), 
        Column('POLICE_DISTRICT', String(10)), 
        Column('ANALYSIS_NEIGHBORHOOD', String(30)), 
        Column('SUPERVISOR_DISTRICT', Float), 
        Column('LATITUDE', Float), 
        Column('LONGITUDE', Float), 
        Column('POINT', String(46)), 
        Column('NEIGHBORHOODS', Float), 
        Column('CURRENT_SUPERVISOR_DISTRICTS', Float), 
        Column('CURRENT_POLICE_DISTRICTS', Float)
    )

    try:
        meta.create_all(engine)
        assert True
    except Exception:
        assert False

