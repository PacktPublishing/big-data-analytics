// Triplets in Graph.   Execute these lines in Scala shell after creating Graph.

graph.triplets.foreach(t => println(t.srcAttr + " is " + t.attr + " of " + t.dstAttr ))

//(Jacob,48) is Father of (Andrew,25)
//(Jacob,48) is Father of (Emily,22)
//(Jessica,45) is Mother of (Andrew,25)
//(Jessica,45) is Mother of (Emily,22)
//(Andrew,25) is Son of (Jessica,45)
//(Ryan,53) is Friend of (Jacob,48)
//(Jacob,48) is Husband of (Jessica,45)
//(Jessica,45) is Wife of (Jacob,48)
//(Andrew,25) is Son of (Jacob,48)
//(Emily,22) is Daughter of (Jacob,48)
//(Emily,22) is Daughter of (Jessica,45)
//(Lily,52) is Sister of (Jacob,48)
