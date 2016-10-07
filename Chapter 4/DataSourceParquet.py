# Copy input dataset(people.parquet) to HDFS  and then get into PySpark Shell. To create a DataFrame from Parquet file and issue SQL commands, use the following command.  

df_parquet = spark.read.load("parquet_dir")
df_parquet.createOrReplaceTempView("parquet_table");
teenagers = spark.sql("SELECT name from parquet_table where age >= 13 AND age <= 19")


# To write the data from parquet DataFrame to json format, use one of the write  commands below. 

df_parquet.write.json("myjson_dir")
df_parquet.write.format("json").save("myjson_dir2")

# There are multiple modes while writing data. By default, ‘error’ mode is enabled which will throw an error if output is already existing. Other modes are append, overwrite and ignore when the target data source already exists. Below example appends data to myjson_dir and parquet_dir directory. 

df_parquet.write.mode("append").json("myjson_dir")
df_parquet.write.mode("append").save("parquet_dir")


# A DataFrame can be written to a Hive Table.  Below example writes data to hive managed table in default Parquet format. 

df_parquet.write.saveAsTable("hive_parquet_table")

# Parquet supports schema evolution like ProtocolBuffer, Avro, and Thrift. Multiple schemas can be merged by setting the global option spark.sql.parquet.mergeSchema to true or use mergeSchema option as shown below. parquet_partitioned is the directory name.

df_parquet = spark.read.option("mergeSchema", "true").parquet("parquet_partitioned")

