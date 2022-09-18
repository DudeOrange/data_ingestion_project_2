
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
import sys

PG_HOST = sys.argv[1]
PG_USER = sys.argv[2]
PG_PASSWORD = sys.argv[3]
PG_DATABASE = sys.argv[4]
PG_PORT = sys.argv[5]
OUTPUT_FILE = sys.argv[6]


url = f'jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DATABASE}'
properties = {'user': PG_USER, 'password': PG_PASSWORD, 'driver': 'org.postgresql.Driver'}

spark = SparkSession.builder.appName('from_postgres_to_parquet_file').getOrCreate()


df = spark.read.jdbc(url=url, table='police_data', properties=properties)

#Запиысваем в паркет-файлы с партицированием по году
df.write.partitionBy('INCIDENT_YEAR').parquet(OUTPUT_FILE)