# Download books.xml as shown below and copy it to HDFS.  Get into PySpark Shell using packages option. 

# [cloudera@quickstart ~]$ wget https://raw.githubusercontent.com/
databricks/spark-xml/master/src/test/resources/books.xml --nocheck-
certificate

#[cloudera@quickstart ~]$ hadoop fs -put books.xml

# [cloudera@quickstart spark-2.0.0-bin-hadoop2.7]$ bin/pyspark
--master yarn --packages com.databricks:spark-xml_2.11:0.4.0

# Read the xml file and select data elements from DataFrame. 

df_xml = spark.read.format('com.databricks.spark.xml').options
(rowTag='book',attributePrefix='@').load('books.xml')

df_xml.select('@id','author','title').show(5,False)

df_xml.where(df_xml.author == "Corets, Eva").select("@id",
"author", "title", "price").withColumn("new_price",df_xml.price *
10).drop("price").show()
