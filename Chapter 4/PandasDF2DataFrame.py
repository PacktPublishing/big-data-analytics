# Pandas is a python library for data manipulation. Use below commands to create a DataFrame in Pandas and convert to Spark DataFrame and vice versa.  Install pandas using pip if it is not done already. 

import pandas
data = [
('v1', 'v5', 'v9'),
('v2', 'v6', 'v10'),
('v3', 'v7', 'v11'),
('v4', 'v8', 'v12') ]

pandas_df = pandas.DataFrame(data, columns=['col1', 'col2', 'col3'])

spark_df = spark.createDataFrame(pandas_df)

spark_df.toPandas()
