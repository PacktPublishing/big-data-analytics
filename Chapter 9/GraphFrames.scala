// On Spark 2.0 cluster, let’s start a spark shell with packages option.  

$SPARK_HOME/bin/spark-shell --packages graphframes:graphframes:0.2.0-spark2.0-s_2.11

import org.graphframes._

val vertex = spark.createDataFrame(List(
    ("1","Jacob",48),
    ("2","Jessica",45),
    ("3","Andrew",25),
    ("4","Ryan",53),
    ("5","Emily",22),
    ("6","Lily",52)
)).toDF("id", "name", "age")


val edges = spark.createDataFrame(List(
    ("6","1","Sister"),
    ("1","2","Husband"),
    ("2","1","Wife"),
    ("5","1","Daughter"),
    ("5","2","Daughter"),
    ("3","1","Son"),
    ("3","2","Son"),
    ("4","1","Friend"),
    ("1","5","Father"),
    ("1","3","Father"),
    ("2","5","Mother"),
    ("2","3","Mother")
)).toDF("src", "dst", "relationship")

val graph = GraphFrame(vertex, edges)

// Once graph is created, all graph operations and algorithms can be executed. Some of the basic operations are shown below.  

graph.vertices.show()

graph.edges.show()

graph.vertices.groupBy().min("age").show()

// Motif finding algorithm is used to search for structural patterns in a graph. GraphFrame based motif finding uses DataFrame based DSL for finding structural patterns.  The following example graph.find("(a)-[e]->(b); (b)-[e2]->(a)") will search for pairs of vertices a,b connected by edges in both directions. It will return a DataFrame of all such structures in the graph, with columns for each of the named elements (vertices or edges) in the motif. In this case, the returned columns will be “a, b, e, e2”.

val motifs = graph.find("(a)-[e]->(b); (b)-[e2]->(a)")
motifs.show()
motifs.filter("b.age > 30").show()
// Loading and Saving of GraphFrames

graph.vertices.write.parquet("vertices")
graph.edges.write.parquet("edges")

val verticesDF = spark.read.parquet("vertices")
val edgesDF = spark.read.parquet("edges")
val sameGraph = GraphFrame(verticesDF, edgesDF)
