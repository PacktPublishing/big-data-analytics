// Triangle Counting in Graph.   Execute these lines in Scala shell after creating Graph.

val tCount = graph.triangleCount().vertices
println( tCount.collect().mkString("\n") )

// The results of the application job show that the vertices called, Lily (6) and Ryan (4), have no triangles, whereas Jacob (1) and Jessica (2) have the most, as expected, as they have the most relationships.
//(4,0)
//(6,0)
//(2,4)
//(1,4)
//(3,2)
//(5,2)
