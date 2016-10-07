# Get into PySpark Shell and execute below commands. 
Create DataFrame and convert to RDD  

mylist = [(50, "DataFrame"),(60, "pandas")]
myschema = ['col1', 'col2']
df = spark.createDataFrame(mylist, myschema)

#Convert DF to RDD
df.rdd.collect()

df2rdd = df.rdd
df2rdd.take(2)