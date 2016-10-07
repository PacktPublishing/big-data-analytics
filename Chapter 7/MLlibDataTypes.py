# Dense vector is a traditional array of doubles.  
import numpy as np
import scipy.sparse as sps
from pyspark.mllib.linalg import Vectors
dv1 = np.array([2.0, 0.0, 5.0])
dv1
#array([ 2.,  0.,  5.])

# Sparse vector uses integer indices and double values.  

sv1 = Vectors.sparse(2, [0, 3], [5.0, 1.0])
sv1
#SparseVector(2, {0: 5.0, 3: 1.0})

# Labeled Point:  This can be dense or Sparse vector with a label used in supervised learning.  
from pyspark.mllib.linalg import SparseVector
from pyspark.mllib.regression import LabeledPoint

# Labeled point with a positive label and a dense feature vector
lp_pos = LabeledPoint(1.0, [4.0, 0.0, 2.0])
lp_pos
# LabeledPoint(1.0, [4.0,0.0,2.0])

# Labeled point with a negative label and a sparse feature vector
lp_neg = LabeledPoint(0.0, SparseVector(5, [1, 2], [3.0, 5.0]))
lp_neg
#LabeledPoint(0.0, (5,[1,2],[3.0,5.0]))

# Local Matrix:  This is a matrix with integer type indices and double type values.  This is also stored on single machine.  
from pyspark.mllib.linalg import Matrix, Matrices

# Dense matrix ((1.0, 2.0, 3.0), (4.0, 5.0, 6.0))
dMatrix = Matrices.dense(2, 3, [1, 2, 3, 4, 5, 6])
# Sparse matrix ((9.0, 0.0), (0.0, 8.0), (0.0, 6.0))
sMatrix = Matrices.sparse(3, 2, [0, 1, 3], [0, 2, 1], [9, 6, 8])
