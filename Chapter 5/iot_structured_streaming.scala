//Let’s create three input datasets as shown below in Json format. In the real-life project, data //would be automatically come to this directory. But, for our understanding, let’s create these //datasets manually.

iot-file1.json
{"device_id":1,"timestamp":1470009600,"status":"active"}
{"device_id":2,"timestamp":1470013200,"status":"active"}
{"device_id":3,"timestamp":1470016800,"status":"active"}
{"device_id":4,"timestamp":1470020400,"status":"active"}
{"device_id":5,"timestamp":1470024000,"status":"active"}
{"device_id":1,"timestamp":1470009601,"status":"active"}
{"device_id":2,"timestamp":1470013202,"status":"active"}
{"device_id":3,"timestamp":1470016803,"status":"inactive"}
{"device_id":4,"timestamp":1470020404,"status":"active"}
{"device_id":5,"timestamp":1470024005,"status":"active"}

iot-file2.json
{"device_id":1,"timestamp":1470027600,"status":"active"}
{"device_id":2,"timestamp":1470031200,"status":"active"}
{"device_id":3,"timestamp":1470034800,"status":"active"}
{"device_id":4,"timestamp":1470038400,"status":"active"}
{"device_id":5,"timestamp":1470042000,"status":"active"}
{"device_id":1,"timestamp":1470027601,"status":"active"}
{"device_id":2,"timestamp":1470031202,"status":"active"}
{"device_id":3,"timestamp":1470034803,"status":"active"}
{"device_id":4,"timestamp":1470038404,"status":"active"}
{"device_id":5,"timestamp":1470042005,"status":"active"}

iot-file3.json
{"device_id":1,"timestamp":1470027601,"status":"active"}

// Create a HDFS directory and copy the first IOT events file.

hadoop fs -mkdir iotstream
hadoop fs -put iot-file1.json iotstream/

// Start a Scala shell session and create the streaming DataFrame and start the stream. bin/spark-  // shell to start the scala shell which creates a pre-configured Spark session called as ‘spark’.


import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

val iotSchema = new StructType().add("device_id", LongType).add("timestamp", TimestampType).add("status", StringType)
val iotPath = "hdfs://quickstart.cloudera:8020/user/cloudera/iotstream"
val iotStreamingDataFrame = spark.readStream.schema(iotSchema).option("maxFilesPerTrigger", 1).json(iotPath)
val iotStreamingCounts    = iotStreamingDataFrame.groupBy($"status", window($"timestamp", "1 hour")).count() 
iotStreamingCounts.isStreaming

val iotQuery =  iotStreamingCounts.writeStream.format("memory").queryName("iotstream").outputMode("complete").start()

//Now query the in-memory iotstream table

spark.sql("select status, date_format(window.start, 'MMM-dd HH:mm') as start_time, date_format(window.end, 'MMM-dd HH:mm') as end_time, count from iotstream order by start_time,end_time, status").show()

// Now, copy the second iot-file2.json to same HDFS directory and you can observe that streaming query executes automatically and computes the counts. Let’s run the same query again as above.

hadoop fs -put iot-file2.json iotstream/

spark.sql("select status, date_format(window.start, 'MMM-dd HH:mm') as start_time, date_format(window.end, 'MMM-dd HH:mm') as end_time, count from iotstream order by start_time,end_time, status").show()

// Now, copy the third iot-file3.json to same HDFS directory which has a late event. Once you execute the query you can observe that late event is handled by updating the previous record.


hadoop fs -put iot-file3.json iotstream/
spark.sql("select status, date_format(window.start, 'MMM-dd HH:mm') as start_time, date_format(window.end, 'MMM-dd HH:mm') as end_time, count from iotstream order by start_time,end_time, status").show()


iotQuery.stop
