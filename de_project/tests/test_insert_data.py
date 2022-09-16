import pytest
import pandas as pd
from sqlalchemy import create_engine, Table, Column, Integer, String, DateTime, Time, Float, Boolean, BigInteger, SMALLINT, MetaData

host = 'localhost'
user = 'user'
password = 'password1'
port = 5432
db = 'policedb'

name_columns = ('INCIDENT_DATETIME', 'INCIDENT_DATE', 'INCIDENT_TIME', 'INCIDENT_YEAR', 'INCIDENT_DAY_OF_WEEK', 'REPORT_DATETIME', 'ROW_ID', 
        'INCIDENT_ID', 'INCIDENT_NUMBER', 'CAD_NUMBER', 'REPORT_TYPE_CODE', 'REPORT_TYPE_DESCRIPTION', 'FILED_ONLINE', 'INCIDENT_CODE',
        'INCIDENT_CATEGORY', 'INCIDENT_SUBCATEGORY', 'INCIDENT_DESCRIPTION', 'RESOLUTION', 'INTERSECTION', 'CNN', 'POLICE_DISTRICT',
        'ANALYSIS_NEIGHBORHOOD', 'SUPERVISOR_DISTRICT', 'LATITUDE', 'LONGITUDE', 'POINT', 'NEIGHBORHOODS', 'CURRENT_SUPERVISOR_DISTRICTS', 'CURRENT_POLICE_DISTRICTS')

test_report_type_code = [
    pd.DataFrame([['2021/05/14 01:51:00 AM', '2021/05/14', '01:51', 2021, 'Friday', '2021/05/14 01:57:00 AM', 103010326030, 1030103, 210295348, 
                    211340138.0, 'II', 'Initial', 'False', 26030, 'Arson', 'Arson', 'Arson', 'Open or Active', '03RD ST \ CUSTER AVE', 20240000.0, 
                    'Bayview', 'Bayview Hunters Point', 10.0, 37.744259, -122.387373, 'POINT (-122.38737260846696 37.74425940578451)', 56.0, 9.0, 2.0],
                    ['2021/05/14 01:51:00 AM', '2021/05/14', '01:51', 2021, 'Friday', '2021/05/14 01:57:00 AM', 103010326031, 1030103, 210295348, 
                    211340138.0, 5, 'Initial', 'False', 26030, 'Arson', 'Arson', 'Arson', 'Open or Active', '03RD ST \ CUSTER AVE', 20240000.0, 
                    'Bayview', 'Bayview Hunters Point', 10.0, 37.744259, -122.387373, 'POINT (-122.38737260846696 37.74425940578451)', 56.0, 9.0, 2.0],
                    ['2021/05/14 01:51:00 AM', '2021/05/14', '01:51', 2021, 'Friday', '2021/05/14 01:57:00 AM', 103010326032, 1030103, 210295348, 
                    211340138.0, 'abc', 'Initial', 'False', 26030, 'Arson', 'Arson', 'Arson', 'Open or Active', '03RD ST \ CUSTER AVE', 20240000.0, 
                    'Bayview', 'Bayview Hunters Point', 10.0, 37.744259, -122.387373, 'POINT (-122.38737260846696 37.74425940578451)', 56.0, 9.0, 2.0],
                    ['2021/05/14 01:51:00 AM', '2021/05/14', '01:51', 2021, 'Friday', '2021/05/14 01:57:00 AM', 103010326033, 1030103, 210295348, 
                    211340138.0, 123, 'Initial', 'False', 26030, 'Arson', 'Arson', 'Arson', 'Open or Active', '03RD ST \ CUSTER AVE', 20240000.0, 
                    'Bayview', 'Bayview Hunters Point', 10.0, 37.744259, -122.387373, 'POINT (-122.38737260846696 37.74425940578451)', 56.0, 9.0, 2.0]
                    ], columns=name_columns)
]


@pytest.fixture()
def engine():
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    return engine

@pytest.fixture(autouse=True)
def create_table(engine):
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

    meta.create_all(engine)



@pytest.mark.parametrize('rows', test_report_type_code)
def test_report_type_code(engine, rows):


    connection = engine.connect()
    transaction = connection.begin()


    try:
        rows.to_sql('temp_table', connection, if_exists='append', index=False)
#        transaction.commit()
        assert True
    except Exception:
        transaction.rollback()
        assert False
    
