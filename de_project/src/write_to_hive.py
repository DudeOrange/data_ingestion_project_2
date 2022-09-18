
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

OUTPUT_FILE = sys.argv[1]



spark = SparkSession.builder.appName('from_parquet_to_hive_table').config('hive.metastore.uris', 'thrift://hive-metastore:9083'). \
                enableHiveSupport().getOrCreate()


spark.sql("DROP TABLE IF EXISTS police_data")

spark.sql(f"""CREATE EXTERNAL TABLE IF NOT EXISTS police_data(
            INCIDENT_DATETIME timestamp, 
            INCIDENT_DATE timestamp, 
            INCIDENT_TIME timestamp, 
            INCIDENT_DAY_OF_WEEK string, 
            REPORT_DATETIME timestamp, 
            ROW_ID bigint, 
            INCIDENT_ID int, 
            INCIDENT_NUMBER int, 
            CAD_NUMBER double, 
            REPORT_TYPE_CODE string, 
            REPORT_TYPE_DESCRIPTION string, 
            FILED_ONLINE boolean, 
            INCIDENT_CODE int, 
            INCIDENT_CATEGORY string, 
            INCIDENT_SUBCATEGORY string, 
            INCIDENT_DESCRIPTION string, 
            RESOLUTION string, 
            INTERSECTION string, 
            CNN double, 
            POLICE_DISTRICT string, 
            ANALYSIS_NEIGHBORHOOD string, 
            SUPERVISOR_DISTRICT double, 
            LATITUDE double, 
            LONGITUDE double, 
            POINT string, 
            NEIGHBORHOODS double, 
            CURRENT_SUPERVISOR_DISTRICTS double, 
            CURRENT_POLICE_DISTRICTS double) 
            PARTITIONED BY (INCIDENT_YEAR int) STORED AS PARQUET LOCATION '{OUTPUT_FILE}'""")


spark.sql("""MSCK REPAIR TABLE police_data""")

#проверяем что таблица создана и все данные на месте
df = spark.sql("select * from police_data")
df.agg(f.count(f.col('ROW_ID'))).show()

