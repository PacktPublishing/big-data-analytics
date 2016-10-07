//Accessing metadata information about Hive tables and UDFs is made easy with Catalog API. //Following commands explain how to access metadata. 

spark.catalog.listDatabases().select("name").show()

spark.catalog.listTables.show()

spark.catalog.isCached("sample_07")

spark.catalog.listFunctions().show() 
