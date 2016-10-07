/* Download the flight data from below location. 
// http://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=236&DB_Short_Name=On-Time
   Select OriginAirportID, Origin, DestAirportID, Dest, and Distance then click download.  Copy the zip file on to the cluster, unzip it and then copy it to HDFS.   
unzip 355968671_T_ONTIME.zip

hadoop fs -put 355968671_T_ONTIME.csv

1.	Get into the Scala shell using spark-shell --master local[2] command and then import all dependencies. 
*/

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

case class Flight(org_id:Long, origin:String, dest_id:Long, dest:String, dist:Float)

def parseFlightCsv(str: String): Flight = {
  val line = str.split(",")
  Flight(line(0).toLong, line(1), line(2).toLong, line(3), line(4).toFloat)
}

val csvRDD = sc.textFile("355968671_T_ONTIME.csv")
val noHdrRDD = csvRDD.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

val flightsRDD = noHdrRDD.map(parseFlightCsv)

val airports = flightsRDD.map(flight => (flight.org_id, flight.origin)).distinct  

airports.take(3)

val default = "nowhere"

val airportMap = airports.map { case ((org_id), name) => (org_id -> name) }.collect.toList.toMap

val flightRoutes = flightsRDD.map(flight => ((flight.org_id, flight.dest_id), flight.dist)).distinct

val edges = flightRoutes.map {
case ((org_id, dest_id), distance) =>Edge(org_id.toLong, dest_id.toLong, distance) }

edges.take(1)

// Array(Edge(10299,10926,160))


val graph = Graph(airports, edges, default)

// Let’s do some basic analytics using this graph.  Find which airports have most incoming flights. 

val maxIncoming = graph.inDegrees.collect.sortWith(_._2 > _._2).map(x => (airportMap(x._1), x._2)).take(3)

maxIncoming.foreach(println)

//(ATL,152)
//(ORD,145)
//(DFW,143)

//Now, let’s find out which airport has the most outgoing flights?

val maxout= graph.outDegrees.join(airports).sortBy(_._2._1, ascending=false).take(3)

maxout.foreach(println)

//(10397,(153,ATL))
//(13930,(146,ORD))
//(11298,(143,DFW))

//Find top 3 flights from a source airport to destination airport. 

graph.triplets.sortBy(_.attr, ascending=false).map(triplet => "There were " + triplet.attr.toInt + " flights from " + triplet.srcAttr + " to " + triplet.dstAttr + ".").take(3) .foreach(println)

//There were 4983 flights from "JFK" to "HNL".
//There were 4983 flights from "HNL" to "JFK".
//There were 4962 flights from "EWR" to "HNL".
