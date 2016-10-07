# Copy input dataset(people.json) to HDFS and then get into PySpark Shell. Create a DataFrame from Json file and then convert it to ORC format.

df_json = spark.read.json("people.json")
df_json.show()

# Create an ORC file by writing data from json DataFrame. 

df_json.write.orc("myorc_dir")
df_json.write.format("orc").save("myorc_dir")

# Read the ORC files with generic read function. 

df_orc = spark.read.orc("myorc_dir")
df_orc = spark.read.load("myorc_dir", format="orc")

# Write the data to hive managed table in orc format. 

df_json.write.saveAsTable("hive_orc_table","orc")

# Enable Predicate Push-down.
  
spark.setConf("spark.sql.orc.filterPushdown", "true")
spark.conf.get("spark.sql.orc.filterPushdown")

# To create ORC table in Hive partitioning data by a column, use below syntax.

df_json.write.format("orc").partitionBy("age").save("partitioned_orc")
