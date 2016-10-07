/*
1.	Create a vertex file with vertex id, name and age as shown below.   

[cloudera@quickstart ~]$ cat vertex.csv 
1,Jacob,48
2,Jessica,45
3,Andrew,25
4,Ryan,53
5,Emily,22
6,Lily,52

2.	Create an edge file with vertex id, destination vertex id and relationship as shown below. 
	
[cloudera@quickstart ~]$ cat edges.csv 
6,1,Sister
1,2,Husband
2,1,Wife
5,1,Daughter
5,2,Daughter
3,1,Son
3,2,Son
4,1,Friend
1,5,Father
1,3,Father
2,5,Mother
2,3,Mother  

3.	Now, let’s copy both files to HDFS.  
[cloudera@quickstart ~]$ hadoop fs -put vertex.csv 
[cloudera@quickstart ~]$ hadoop fs -put edges.csv

4.	Start Scala shell with master as yarn-client and then import Graphx and RDD dependencies. 

[cloudera@quickstart ~]$ spark-shell --master local[2]

scala> sc.master
res0: String = local[2]
*/

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

// 5.	Create an RDD for both vertex and edge files.

val vertexRDD = sc.textFile("vertex.csv")

vertexRDD.collect()

val edgeRDD =  sc.textFile("edges.csv")

edgeRDD.collect()

// 6.	Let’s create VertexRDD with VertexId, and strings to represent the person's name and age. 

val vertices: RDD[(VertexId, (String, String))] =
          vertexRDD.map { line =>
          val fields = line.split(",")
       ( fields(0).toLong, ( fields(1), fields(2) ) )
       }

vertices.collect()

// 7.	Let’s create EdgeRDD with source and destination vertex IDs converted to Long values and relationship as string. Each record in this RDD is now an Edge record.   

val edges: RDD[Edge[String]] =
          edgeRDD.map { line =>
          val fields = line.split(",")
          Edge(fields(0).toLong, fields(1).toLong, fields(2))
       }

edges.collect()

// 8.	We should define a default value in case a connection or a vertex is missing.  Graph is then constructed from these RDDs - vertices, edges, and the default record.

val default = ("Unknown", "Missing")

val graph = Graph(vertices, edges, default)
