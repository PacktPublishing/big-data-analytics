// Enter to Mahout's Scala shell using commans
// bin/mahout spark-shell
// The following two Scala imports are typically used to enable Mahout Scala DSL Bindings, for linear algebra. 

import org.apache.mahout.math._
import scalabindings._
import MatlabLikeOps._

// Dense vector: The dense vector is a vector with relatively fewer zero elements. On the Mahout command line, please type the following command to initialize a dense vector:

val denseVector1: Vector = (3.0, 4.1, 6.2)

//denseVector1: org.apache.mahout.math.Vector = {0:3.0,1:4.1,2:6.2}

// Sparse vector: Sparse vector is a vector with a relatively large number of zero elements. On the Mahout command line, please type the following command to initialize a sparse vector:

val sparseVector1 = svec((6 -> 1) :: (9 -> 2.0) :: Nil)
//sparseVector1: org.apache.mahout.math.RandomAccessSparseVector = {9:2.0,6:1.0}


// Access elements of vector:
denseVector1(2)

//res0: Double = 6.2

// Set values to a vector
denseVector1(2)=8.2
denseVector1

//res2: org.apache.mahout.math.Vector = {0:3.0,1:4.1,2:8.2}

mahout> val denseVector2: Vector = (1.0, 1.0, 1.0)

//denseVector2: org.apache.mahout.math.Vector = {0:1.0,1:1.0,2:1.0}


// Vector arithmetic operations:
val addVec=denseVector1 +  denseVector2

//addVec: org.apache.mahout.math.Vector = {0:4.0,1:5.1,2:9.2}

val subVec=denseVector1 -  denseVector2

subVec: org.apache.mahout.math.Vector = {0:2.0,1:3.0999999999999996,2:7.199999999999999}

//Similarily, multiplication and division also can be done.  

//The result of adding a scalar to a vector is that all elements are incremented by the value of the scalar. For example, the following command adds 5 to all the elements of the vector.  

val addScalr=denseVector1+10
// addScalr: org.apache.mahout.math.Vector = {0:13.0,1:14.1,2:18.2}

val addScalr=denseVector1-2
//addScalr: org.apache.mahout.math.Vector = {0:1.0,1:2.0999999999999996,2:6.199999999999999}

//Let’s see how to initialize the matrix now.  The inline initialization of a matrix, either dense or sparse, is always performed row-wise.

//Dense Matrix:
val denseMatrix = dense((10, 20, 30), (30, 40, 50))

// Sparse Matrix:
val sparseMatrix = sparse((1, 30) :: Nil, (0, 20) :: (1, 20.5) :: Nil)

// Diagonal matrix:
val diagonalMatrix=diag(20, 4)

// Identity matrix:
val identityMatrix = eye(4)

// Accessing elements of matrix:

denseMatrix(1,1)
//res5: Double = 40.0

sparseMatrix(0,1)
//res18: Double = 30.0

//Fetching a row.
denseMatrix(1,::)
//res21: org.apache.mahout.math.Vector = {0:30.0,1:40.0,2:50.0}

//Fetching a column
denseMatrix(::,1)
//res22: org.apache.mahout.math.Vector = {0:20.0,1:40.0}

// Setting the matrix row.  
denseMatrix(1,::)=(99,99,99)
//res23: org.apache.mahout.math.Vector = {0:99.0,1:99.0,2:99.0}
denseMatrix


// Matrices are assigned by reference and not as a copy.  See the example below.
val newREF = denseMatrix
newREF += 10.0
denseMatrix

// If you want a separate copy, you can clone it. 

val newClone = denseMatrix clone
newClone += 10
newClone
denseMatrix