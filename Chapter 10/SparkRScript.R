# Submit the script using spark-submit command and check the result on HDFS.  
# [cloudera@quickstart ~]$ cd spark-2.0.0-bin-hadoop2.7
# [cloudera@quickstart spark-2.0.0-bin-hadoop2.7]$ bin/spark-submit ~/sparkrScript.R


library(SparkR)

# Initialize SparkSession
sparkR.session(appName = "SparkR Script")

# Create a local data frame in R
localDataFrameinR <- data.frame(name=c("Jacob", "Jessica", "Andrew"), age=c(48, 45, 25))

# Convert R’s local data frame to a SparkR’s distributed DataFrame
DataFrameSparkR <- createDataFrame(localDataFrameinR)

# Print SparkR DataFrame schema
printSchema(DataFrameSparkR)

# Print the rows
head(DataFrameSparkR)

# Register the DataFrame as table
createOrReplaceTempView(DataFrameSparkR, "sparkrtemptable")

# SQL statements on registered table, convert to local dataframe and print
age25above <- sql("SELECT name FROM sparkrtemptable WHERE age > 25")
age25abovelocaldf <- collect(age25above)
print(age25abovelocaldf)

# Write the data out in json format on HDFS. 
write.df(DataFrameSparkR, path="SparkR.json", source="json", mode="overwrite")

# Stop Script
sparkR.stop()

