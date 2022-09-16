import os
from pyspark.sql import SparkSession
#from pyspark.sql import functions as f
#from pyspark.sql.window import Window as w
#from pyspark.sql import DataFrame
import sys

print(sys.argv[1])
print(sys.argv[2])
print(sys.argv[3])
print(sys.argv[4])
print(sys.argv[5])

#JAVA_HOME = os.environ['JAVA_HOME']

#print(f'{JAVA_HOME}jars/postgresql-42.3.4.jar')
print(os.path.exists('/opt/airflow/jar'))
print(os.path.isfile('/opt/airflow/jar/postgresql-42.3.4.jar'))

#spark = SparkSession.builder.config('spark.driver.extraClassPath', '/opt/airflow/jar/postgresql-42.3.4.jar') \
#        .config('spark.executor.extraclasspath', '/opt/airflow/jar/postgresql-42.3.4.jar').config('driver', 'org.postgresql.Driver').getOrCreate()

spark = SparkSession.builder.getOrCreate()

#spark = SparkSession.builder.config('spark.driver.extraClassPath', '/opt/airflow/jar/postgresql-42.3.4.jar') \
#        .config('spark.executor.extraclasspath', '/opt/airflow/jar/postgresql-42.3.4.jar').getOrCreate()

url = f'jdbc:postgresql://{sys.argv[1]}:{sys.argv[5]}/{sys.argv[4]}'
properties = {'user':sys.argv[2], 'password': sys.argv[3], 'fetchsize': '100', 'driver': 'org.postgresql.Driver'}

print(url)
print(properties)

df = spark.read.jdbc(url=url, table='police_data', properties=properties)

df.show(3)