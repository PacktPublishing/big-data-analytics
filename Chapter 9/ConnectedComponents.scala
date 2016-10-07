// Connected Components in Graph.   Execute these lines in Scala shell after creating Graph.

val iterations = 1000
val connected = graph.connectedComponents().vertices
val connectedS = graph.stronglyConnectedComponents(iterations).vertices

//The vertex counts are then joined with the original vertex records, so that the connection counts can be associated with the vertex information, such as the person's name.

val connByPerson = vertices.join(connected).map {
case (id, ( (person,age) , conn )) => (conn, id, person)
}

val connByPersonS = vertices.join(connectedS).map {
case (id, ( (person,age) , conn )) => (conn, id, person)
}

//The results are then output using a case statement, and formatted printing.

connByPerson.collect().foreach {
  case (conn, id, person) =>
println ( f"Weak $conn $id $person" )
 }

//Weak 1 4 Ryan
//Weak 1 6 Lily
//Weak 1 2 Jessica
//Weak 1 1 Jacob
//Weak 1 3 Andrew
//Weak 1 5 Emily

connByPersonS.collect().foreach {
 case (conn, id, person) =>
println ( f"Strong $conn $id $person" )
 }

//The result for the stronglyConnectedComponents algorithm output is as follows. 
//Strong 4 4 Ryan
//Strong 6 6 Lily
//Strong 1 2 Jessica
//Strong 1 1 Jacob
//Strong 1 3 Andrew
//Strong 1 5 Emily

