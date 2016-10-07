import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

val spark = SparkSession
  .builder
  .appName("CSVStructuredStreaming")
  .getOrCreate()

val csvSchema = new StructType().add("emp_name", "string").add("emp_age", "integer")
val csv_df = spark
    .readStream
    .option("sep", ",")
    .schema(userSchema)
    .csv("HDFS Path")

csv_df
   .writeStream
   .format("parquet")
   .option("path","HDFS Path")
   .option("checkpointLocation","HDFS Path")
   .outputMode("append")
   .start()
