# Create Kafka topic and send messages to topic

cd /usr/hdp/current/kafka-broker/bin/

./kafka-topics.sh --zookeeper localhost:2181 --topic ambarilogs --create --partitions 2 --replication-factor 1

tail -f /var/log/ambari-agent/ambari-agent.log | ./kafka-console-producer.sh --broker-list sandbox.hortonworks.com:6667 --topic ambarilogs

# Download spark-reciever and site-to-site client jars. Check Nifi version and download compatible versions. This example downloads 0.5.1 version related jars.

mkdir /opt/spark-receiver
cd /opt/spark-receiver
wget http://central.maven.org/maven2/org/apache/nifi/nifi-site-to-site-client/0.5.1/nifi-site-to-site-client-0.5.1.jar
wget http://central.maven.org/maven2/org/apache/nifi/nifi-spark-receiver/0.5.1/nifi-spark-receiver-0.5.1.jar

#In Ambari, go to Spark service configurations and add below two properties in custom spark-defaults and then restart Spark service.

spark.driver.allowMultipleContexts true
spark.driver.extraClassPath  /opt/spark-receiver/nifi-spark-receiver-0.5.1.jar:/opt/spark-receiver/nifi-site-to-site-client-0.5.1.jar:/opt/nifi-0.5.1.1.1.2.0-32/lib/nifi-api-0.5.1.1.1.2.0-32.jar:/opt/nifi-0.5.1.1.1.2.0-32/lib/bootstrap/nifi-utils-0.5.1.1.1.2.0-32.jar:/opt/nifi-0.5.1.1.1.2.0-32/work/nar/framework/nifi-framework-nar-0.5.1.1.1.2.0-32.nar-unpacked/META-INF/bundled-dependencies/nifi-client-dto-0.5.1.1.1.2.0-32.jar

# In Ambari NiFi configurations, change below site to site configurations and restart the service.

nifi.remote.input.socket.host=
nifi.remote.input.socket.port=8055
nifi.remote.input.secure=false

# On the Nifi UI (ipaddressofsandbox:9090/nifi), drag a processor and choose and add GetKafka processor. Right-click on the processor and click on configure. Enter the Zookeeper connection string as ipaddressofsandbox:2181 (example: 192.168.139.167:2181) and topic as ambarilogs. Add another PutHDFS processor and configure to write to the /tmp/kafka directory and specify the configuration resources as /etc/hadoop/conf/core-site.xml,/etc/hadoop/conf/hdfs-site.xml. Set auto terminate relationships in HDFS processor. Also, drag an output port and name it as Data for Spark. Connect the processors and start the dataflow. Make sure to remove all warnings that are shown at the top of the processor. Now, let’s create a scala Spark streaming application ambari-logs.sh with below code to pull data from NiFi workflow. 

cd /opt/spark-receiver
vi ambari-logs.sh

import org.apache.nifi._
import java.nio.charset._
import org.apache.nifi.spark._
import org.apache.nifi.remote.client._
import org.apache.spark._
import org.apache.nifi.events._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.nifi.remote._
import org.apache.nifi.remote.client._
import org.apache.nifi.remote.protocol._
import org.apache.spark.storage._
import org.apache.spark.streaming.receiver._
import java.io._
import org.apache.spark.serializer._
import org.apache.nifi.remote.client.SiteToSiteClient
import org.apache.nifi.spark.{NiFiDataPacket, NiFiReceiver}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkNiFiAmbari {
def main(args: Array[String]) {
val conf = new SiteToSiteClient.Builder().url("http://localhost:9090/nifi").portName("Data for Spark").buildConfig()
val config = new SparkConf().setAppName("Ambari Log Analyzer")
val ssc = new StreamingContext(config, Seconds(30))
val packetStream: ReceiverInputDStream[NiFiDataPacket] =
    ssc.receiverStream(new NiFiReceiver(conf, StorageLevel.MEMORY_ONLY))
    val lines: DStream[String] = packetStream.flatMap(packet => new String(packet.getContent).split("\n"))
    val pairs: DStream[(String, Int)] = lines.map(line => (line.split(" ")(0), 1))
    val wordCounts: DStream[(String, Int)] = pairs.reduceByKey(_ + _)

    wordCounts.print()

ssc.start()
}
}
SparkNiFiAmbari.main(Array())
 
# Start the Spark shell by passing the program to it. 

spark-shell -i ambari-logs.sh

# Output will appear on the screen similar to

-------------------------------------------
Time: 1471091460000 ms
-------------------------------------------
(INFO,46)
(WARN,4)
(ERROR,1)

Check HDFS directory (/tmp/kafka) to see the data is being written from Kafka.