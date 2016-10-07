// Joining Graphs.   Execute these lines in Scala shell after creating Graph.

case class MoviesWatched(Movie: String, Genre: String)

val movies: RDD[(VertexId, MoviesWatched)] = sc.parallelize(List( (1, MoviesWatched("Toy Story 3", "kids")), (2, MoviesWatched("Titanic", "Love")), (3, MoviesWatched("The Hangover", "Comedy"))))


val movieOuterJoinedGraph = graph.outerJoinVertices(movies)((_,name, movies) => (name,movies))


movieOuterJoinedGraph.vertices.foreach(println)

//(1,((Jacob,48),Some(MoviesWatched(Waiting to Exhale,Drama))))
//(4,((Ryan,53),None))
//(3,((Andrew,25),Some(MoviesWatched(The Hangover,Comedy))))
//(6,((Lily,52),None))
//(5,((Emily,22),None))
//(2,((Jessica,45),Some(MoviesWatched(Titanic,Romance))))

//You can see that movies information is added for Jacob, Andrew, and Jessica. We can use the getOrElse method to provide default attribute values for the vertices that are not present in the passed vertex.

val movieOuterJoinedGraph = graph.outerJoinVertices(movies)((_,name, movies) => (name,movies.getOrElse(MoviesWatched("NA","NA"))))

movieOuterJoinedGraph.vertices.foreach(println)

//(1,((Jacob,48),MoviesWatched(Waiting to Exhale,Drama)))
//(3,((Andrew,25),MoviesWatched(The Hangover,Comedy)))
//(5,((Emily,22),MoviesWatched(NA,NA)))
//(4,((Ryan,53),MoviesWatched(NA,NA)))
//(6,((Lily,52),MoviesWatched(NA,NA)))
//(2,((Jessica,45),MoviesWatched(Titanic,Romance)))
