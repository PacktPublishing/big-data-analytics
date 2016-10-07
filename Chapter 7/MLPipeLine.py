# Get into the PySpark shell and import all dependencies as shown below. 

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF, Tokenizer
from pyspark.sql import Row

# 1.	Prepare training documents DataFrame from a list of (id, text, label) tuples.

LabeledDocument = Row("id", "text", "label")
training_df = spark.createDataFrame([
        (0L, "apache spark rdd memory", 1.0),
 	     (1L, "mllib pipeline", 0.0),
   	   (2L, "hadoop mahout", 1.0),
     	   (3L, "mapreduce iterative", 0.0)], ["id", "text", "label"])

training_df.printSchema()

training_df.show()

# 2.	Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.

tokenizer_split = Tokenizer(inputCol="text", outputCol="words")
hashingTF_vectors = HashingTF(inputCol=tokenizer_split.getOutputCol(), outputCol="features")
log_reg = LogisticRegression(maxIter=10, regParam=0.01)
pipeline = Pipeline(stages=[tokenizer_split, hashingTF_vectors, log_reg])
  
model = pipeline.fit(training_df)

# 3.	Prepare test documents, which are unlabeled (id, text) tuples.

test_df = spark.createDataFrame([
       (4L, "p q r"),
       (5L, "mllib pipeline"),
       (6L, "x y z"),
       (7L, "hadoop mahout")], ["id", "text"])

test_df.show()

# 4.	Make predictions on test documents and print columns of interest.

prediction = model.transform(test_df)
prediction

# DataFrame[id: bigint, text: string, words: array<string>, features: vector, rawPrediction: vector, probability: vector, prediction: double]

selected = prediction.select("id", "text","probability", "prediction")
selected
# DataFrame[id: bigint, text: string, probability: vector, prediction: double]

for row in selected.collect():
        print(row)
"""  
Row(id=4, text=u'p q r', probability=DenseVector([0.6427, 0.3573]), prediction=0.0)
Row(id=5, text=u'mllib pipeline', probability=DenseVector([0.9833, 0.0167]), prediction=0.0)
Row(id=6, text=u'x y z', probability=DenseVector([0.6427, 0.3573]), prediction=0.0)
Row(id=7, text=u'hadoop mahout', probability=DenseVector([0.0218, 0.9782]), prediction=1.0)
"""