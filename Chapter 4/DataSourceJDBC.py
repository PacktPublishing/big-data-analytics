# Get into PySpark shell. Create DataFrames using one of the two methods below and then work on them.  

#Method1
df1 = spark.read.format('jdbc').options(url='jdbc:mysql://localhost:3306/retail_db?user=root&password=cloudera', dbtable='departments').load()
df1.show()

#Method2
df2 = spark.read.format('jdbc').options(url='jdbc:mysql://localhost:3306/retail_db', dbtable='departments', user='root', password='cloudera').load()
df2.show()

df1.createTempView("dept")
df_new = spark.sql("select * from dept where department_id> 5")

# To Write the DataFrame to another table in MySQL database.  This will create the table and writes the data out.   

df_new.write.jdbc("jdbc:mysql://localhost:3306/retail_db?user=root&password=cloudera","new_table")

# To write the DataFrame to another table in MySQL database and also specify the overwrite option to overwrite the data if the table is already existing.    

df_new.write.jdbc("jdbc:mysql://localhost:3306/retail_db?user=root&password=cloudera","new_table","overwrite")
