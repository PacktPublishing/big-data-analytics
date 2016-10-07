// Pregel API. Run this code in Scala Shell after creating the graph in Flight Analytics. 

//  starting vertex
val sourceId: VertexId = 13024 

// A graph with edges containing airfare cost calculation as 50 + distance / 20.

val gg = graph.mapEdges(e => 50.toDouble + e.attr.toDouble/20  )

//Initialize graph, all vertices except source have distance infinity.

val initialGraph = gg.mapVertices((id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity)

// Now, call pregel on graph.

val sssp = initialGraph.pregel(Double.PositiveInfinity)(
  (id, dist, newDist) => math.min(dist, newDist), 
  triplet => {  
  if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
      Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
    } else {
      Iterator.empty
    }
  },
  (a,b) => math.min(a,b) 
)

// Now, print routes with lowest flight cost.

println(sssp.edges.take(4).mkString("\n"))

//Edge(10135,10397,84.6)
//Edge(10135,13930,82.7)
//Edge(10140,10397,113.45)
//Edge(10140,10821,133.5)

//Find the routes with airport codes and lowest flight cost

sssp.edges.map{ case ( Edge(org_id, dest_id,price))=> ( (airportMap(org_id), airportMap(dest_id), price)) }.takeOrdered(10)(Ordering.by(_._3))

// Array((WRG,PSG,51.55), (PSG,WRG,51.55), (CEC,ACV,52.8), (ACV,CEC,52.8), (ORD,MKE,53.35), (IMT,RHI,53.35), (MKE,ORD,53.35), (RHI,IMT,53.35), (STT,SJU,53.4), (SJU,STT,53.4))

// Find airports with lowest flight cost.

println(sssp.vertices.take(4).mkString("\n"))

//(10208,277.79)
//(10268,260.7)
//(14828,261.65)
//(14698,125.25)

// Find airport codes sorted lowest flight cost.

sssp.vertices.collect.map(x => (airportMap(x._1), x._2)).sortWith(_._2 < _._2)

//res21: Array[(String, Double)] = Array(PDX,62.05), (SFO,65.75), (EUG,117.35)
