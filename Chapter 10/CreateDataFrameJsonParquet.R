# Get into SparkR shell and run these commands.

people_json <- read.df("file:///home/cloudera/spark-2.0.0-bin-hadoop2.7/examples/src/main/resources/people.json", "json")

people_json
DataFrame[age:bigint, name:string]

head(people_json)

# To write the people DataFrame as Parquet file, use below command. This will create people-parq directory on HDFS and creates parquet file with snappy compression.

write.df(people_json, path = "people-parq", source = "parquet", mode = "overwrite")


