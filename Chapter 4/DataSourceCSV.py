# Copy input dataset (cars.csv) to HDFS and then get into PySpark Shell. 

# [cloudera@quickstart spark-2.0.0-bin-hadoop2.7 ]$  wget https://raw.githubusercontent.com/databricks/spark-csv/master/src/test/resources/cars.csv --no-check-certificate

# [cloudera@quickstart spark-2.0.0-bin-hadoop2.7 ]$ hadoop fs -put cars.csv

csv_df = spark.read.options(header='true',inferSchema='true').csv("cars.csv")

csv_df.printSchema()

csv_df.select('year', 'model').write.options(codec="org.apache.hadoop.io.compress.GzipCodec").csv('newcars.csv')