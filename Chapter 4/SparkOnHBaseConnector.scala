// Copy hbase-site.xml to spark configuration directory.

cp /etc/hbase/conf/hbase-site.xml /etc/spark/conf/

/*
Create an HBase table and insert some data into it. Though there is a way to create table and insert data using this connector, it is more common analyze the data on an existing table. 

hbase(main):001:0> create 'IOTEvents', {NAME => 'e', VERSIONS => 100}
hbase(main):002:0> put 'IOTEvents', '100', 'e:status', 'active'
hbase(main):003:0> put 'IOTEvents', '100', 'e:e_time', '1470009600'
hbase(main):004:0> put 'IOTEvents', '200', 'e:status', 'active'
hbase(main):005:0> put 'IOTEvents', '200', 'e:e_time', '1470013200'
hbase(main):006:0> put 'IOTEvents', '300', 'e:status', 'inactive'

*/

// Start spark shell with packages option.

// [cloudera@quickstart spark-2.0.0-bin-hadoop2.7 ]$ spark-shell --packages zhzhan:shc:0.0.11-1.6.1-s_2.10

// Define a catalog with HBase and DataFrame mapping and define a function to read data from HBase. Create a DataFrame using the catalog and read function.

import org.apache.spark.sql.execution.datasources.hbase._

def iotCatalog = s"""{
  |"table":{"namespace":"default", "name":"IOTEvents"},
  |"rowkey":"key",
  |"columns":{
    |"Rowkey":{"cf":"rowkey", "col":"key", "type":"string"},
    |"Status":{"cf":"e", "col":"status", "type":"string"},
    |"Event_time":{"cf":"e", "col":"e_time", "type":"string"}
  |}
|}""".stripMargin

import org.apache.spark.sql._

def readHBase(cat: String): DataFrame = {
  sqlContext
  .read
  .options(Map(HBaseTableCatalog.tableCatalog->cat))
  .format("org.apache.spark.sql.execution.datasources.hbase")
  .load()
}

val iotEventsDF = readHBase(iotCatalog)


iotEventsDF.show()
iotEventsDF.filter($"Status" === "inactive").show()

iotEventsDF.registerTempTable("iotevents")

sqlContext.sql("select count(Rowkey) as count from iotevents").show

sqlContext.sql("select Rowkey, Event_time, Status from iotevents where Status = 'active'").show
