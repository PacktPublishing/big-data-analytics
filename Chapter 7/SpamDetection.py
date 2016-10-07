"""
1. First of all, let’s create some sample spam and ham emails. 
[cloudera@quickstart ~]$ cat spam_messages.txt 
$$$ Send money
100% free 
Amazing stuff 
Home based
Reverses aging
No investment
Send SSN and password

[cloudera@quickstart ~]$ cat ham_messages.txt 
Thank you for attending conference
Message from school
Recommended courses for you
Your order is ready for pickup
Congratulations on your anniversary

2.	Copy the both files to HDFS.
[cloudera@quickstart ~]$ hadoop fs -put spam_messages.txt 
[cloudera@quickstart ~]$ hadoop fs -put ham_messages.txt

3.	Get into pyspark shell with below command.  You can change the master to yarn-client to execute on yarn.  

[cloudera@quickstart ~]$ pyspark --master local[*]
"""

from pyspark import SparkContext
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import LogisticRegressionWithSGD
from pyspark.mllib.feature import HashingTF

# 4.	Create RDDs for spam and ham messages.  
    
spam_messages = sc.textFile("spam_messages.txt")
ham_messages = sc.textFile("ham_messages.txt")

# 5.	Create a HashingTF instance to map email text to vectors of 100 features. Split each email into words and then map each word to one feature.

tf = HashingTF(numFeatures = 100)
spam_features = spam_messages.map(lambda email: tf.transform(email.split(" ")))
ham_features = ham_messages.map(lambda email: tf.transform(email.split(" ")))

# 6.	Create LabeledPoint datasets for positive (spam) and negative (ham) examples. A LabeledPoint consists simply of a label and a features vector.  

positive_examples = spam_features.map(lambda features: LabeledPoint(1, features))
negative_examples = ham_features.map(lambda features: LabeledPoint(0, features))

# 7. Create training data and cache it since Logistic Regression is an iterative algorithm. Examine the training data with collect action. 

training_data = positive_examples.union(negative_examples)
training_data.cache()
training_data.collect()

# 8.	Run Logistic Regression using the SGD optimizer and then check the model contents. 
    
model = LogisticRegressionWithSGD.train(training_data)
model

# 9.	Test on a positive example (which is a spam) and a negative one (which is a ham). Apply the same HashingTF feature transformation algorithm used on the training data.

pos_example = tf.transform("No investment required".split(" "))
neg_example = tf.transform("Data Science courses recommended for you".split(" "))

# 10.	Now use the learned model to predict spam/ham for new emails.

print "Prediction for positive test: %g" % model.predict(pos_example)
# Prediction for positive test: 1
print "Prediction for negative test: %g" % model.predict(neg_example)
# Prediction for negative test: 0
