// Get into Scala shell using
// spark-shell --master local[*] --driver-memory 3G 
// Create Graph first before executing below code.

val tolerance = 0.0001
val ranking = graph.pageRank(tolerance).vertices

val rankByPerson = vertices.join(ranking).map {
          case (id, ( (person,age) , rank )) => (rank, id, person)
          }

rankByPerson.collect().foreach {
case (rank, id, person) =>
println ( f"Rank $rank%1.2f for id $id person $person")
 }

//The output from the application is then shown here. As expected, Jacob and Jessica have the highest rank, as they have the most relationships.  

//Rank 0.15 for id 4 person Ryan
//Rank 0.15 for id 6 person Lily
//Rank 1.62 for id 2 person Jessica
//Rank 1.82 for id 1 person Jacob
//Rank 1.13 for id 3 person Andrew
//Rank 1.13 for id 5 person Emily
