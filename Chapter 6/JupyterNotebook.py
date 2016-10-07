# Use these commands in Jupyter notebook

from operator import add
words = sc.parallelize(["hadoop spark hadoop spark mapreduce spark jupyter ipython notebook interactive analytics"])
counts = words.flatMap(lambda x: x.split(' ')) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(add)   \
                  .sortBy(lambda x: x[1])

%matplotlib inline
import matplotlib.pyplot as plt
def plot(counts):
    labels = map(lambda x: x[0], counts)
    values = map(lambda y: y[1], counts)
    plt.barh(range(len(values)), values, color='green')
    plt.yticks(range(len(values)), labels)
    plt.show()

plot(counts.collect())
