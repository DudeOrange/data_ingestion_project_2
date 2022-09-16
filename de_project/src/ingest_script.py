
import pandas as pd
from sqlalchemy import create_engine, Table, Column, Integer, String, DateTime, Time, Float, Boolean, BigInteger, SMALLINT, MetaData

def data_ingestion(host, user, password, database, port, path):
    """
    Connect to database. Create table and ingest data to the table.
    """

    drop_col = ['ESNCAG_-_BOUNDARY_FILE', 'CENTRAL_MARKET/TENDERLOIN_BOUNDARY_POLYGON_-_UPDATED', 'CIVIC_CENTER_HARM_REDUCTION_PROJECT_BOUNDARY',
            'HSOC_ZONES_AS_OF_2018-06-05', 'INVEST_IN_NEIGHBORHOODS_(IIN)_AREAS']

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
    engine.connect

    meta = MetaData()

    police_data = Table(
    'police_data', meta, 
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

    meta.drop_all(engine)
    meta.create_all(engine)

    df_iter = pd.read_csv(path, iterator=True, chunksize=15000, index_col=False)

    for df in df_iter:
        df.rename(columns=lambda x: x.replace(' ', '_').upper(), inplace=True)

        df.drop(columns = drop_col, inplace=True)

        df.to_sql(name='police_data', con=engine, if_exists='append', index=False)




        