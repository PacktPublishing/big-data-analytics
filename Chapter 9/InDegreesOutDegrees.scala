// In Degress, Out Degrees and Degress.   Execute these lines in Scala shell after creating Graph.

def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId,    Int) = {
     	   if (a._2 > b._2) a else b
      	 }

val maxInDegree: (VertexId, Int)  = graph.inDegrees.reduce(max)

val maxOutDegree: (VertexId, Int) = graph.outDegrees.reduce(max)

val maxDegrees: (VertexId, Int)   = graph.degrees.reduce(max)

// We can define a minimum function to find people with minimum out degrees as below.

val minDegrees = graph.outDegrees.filter(_._2 <= 1)
minDegrees.foreach(println)

//(4,1)
//(6,1)
