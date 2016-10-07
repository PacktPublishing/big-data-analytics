# Copy input dataset (people.json) to HDFS and then get into PySpark Shell using packages option. 

# [root@myhost ~]# pyspark --packages com.databricks:spark-avro_2.11:3.0.0

# Let’s read the JSON file, write it in AVRO format and then read the same file.  

df_json = spark.read.json("people.json")
df_json.write.format("com.databricks.spark.avro").save("avro_out")
df_avro = spark.read.format("com.databricks.spark.avro").load("avro_out")

# You can specify the record name and namespace to use when writing out by passing recordName and recordNamespace as optional parameters to Spark.

df_avro.write.format("com.databricks.spark.avro").option("recordName","AvroTest").option("recordNamespace","com.cloudera.spark").save("newavro")
