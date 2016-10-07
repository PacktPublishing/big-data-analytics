// Filtering in Graph.   Execute these lines in Scala shell after creating Graph.

val cnt1 = graph.vertices.filter { case (id, (name, age)) => age.toLong > 40 }.count

//cnt1: Long = 4

println( "Vertices count : " + cnt1 )
//Vertices count : 4

//Now, filter the edges on the relationship property of Mother or Father and then print the output. 
val cnt2 = graph.edges.filter { case Edge(from, to, property) => property == "Father" | property == "Mother" }.count
//cnt2: Long = 4

println( "Edges count : " + cnt2 )
//Edges count : 4
