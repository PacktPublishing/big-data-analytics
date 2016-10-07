# Get into PySpark shell and execute below commands.

mylist = [(50, "DataFrame"),(60, "pandas")]
myschema = ['col1', 'col2']

# Method1: Create a DataFrame with a list, schema and default data types.
df1 = spark.createDataFrame(mylist, myschema)

# Method2: Create a DataFrame by parallelizing list and convert the RDD to DataFrame.  
df2 = sc.parallelize(mylist).toDF(myschema)

# Method3: Read data from a file, Infer schema and convert to DataFrame. Copy people.txt located in examples/resources directory to hdfs (hadoop fs -put /path/to/people.txt people.txt )

from pyspark.sql import SQLContext, Row
peopleRDD = sc.textFile("people.txt")
people_sp = peopleRDD.map(lambda l: l.split(","))
people = people_sp.map(lambda p: Row(name=p[0], age=int(p[1])))
df_people = spark.createDataFrame(people)
df_people.createOrReplaceTempView("people")
spark.sql("SHOW TABLES").show()
spark.sql("SELECT name,age FROM people where age > 19").show()


# Method4: Read data from file, assign schema programmatically.

from pyspark.sql import SQLContext, Row
peopleRDD = sc.textFile("people.txt")
people_sp = peopleRDD.map(lambda l: l.split(","))
people = people_sp.map(lambda p: Row(name=p[0], age=int(p[1])))
df_people = people_sp.map(lambda p: (p[0], p[1].strip()))
schemaStr = "name age"
fields = [StructField(field_name, StringType(), True) \
for field_name in schemaStr.split()]
schema = StructType(fields)
df_people = spark.createDataFrame(people,schema)
df_people.show()
df_people.createOrReplaceTempView("people")
spark.sql("select * from people").show()


