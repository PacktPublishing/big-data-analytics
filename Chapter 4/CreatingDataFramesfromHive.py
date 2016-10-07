#To create DataFrames from hive, get into PySpark Shell and execute below commands. 

# Method #1
sample_07 = spark.table("sample_07")
sample_07.show()

# Method #2
spark.sql("select * from sample_07 limit 5").show()





