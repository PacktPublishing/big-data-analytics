#To create DataFrame from external databases, use sqlContext.read API with jdbc as format and provide the connect string, table name, user id and password as options.  Get into PySpark Shell and execute below commands. 

#Method1
df1 = spark.read.format('jdbc').options(url='jdbc:mysql://localhost:3306/retail_db?user=root&password=cloudera', dbtable='departments').load()
df1.show()

#Method2
df2 = spark.read.format('jdbc').options(url='jdbc:mysql://localhost:3306/retail_db', dbtable='departments', user='root', password='cloudera').load()
df2.show()
