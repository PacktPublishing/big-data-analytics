# To load text files, we use “text” method which will return a single column with column name as “value” and type as string.

df_txt = spark.read.text("people.txt")
df_txt.show()
df_txt
# DataFrame[value: string]
