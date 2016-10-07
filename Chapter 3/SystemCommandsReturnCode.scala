import sys.process._
val res = "ls /tmp" ! // notice the “!” operator 
println("result = "+res) // result can be zero or non-zero
