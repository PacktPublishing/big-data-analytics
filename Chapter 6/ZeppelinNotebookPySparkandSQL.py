%pyspark
words = sc.textFile('file:///var/log/ambari-agent/ambari-agent.log') \
 .flatMap(lambda x: x.lower().split(' ')) \
 .filter(lambda x: x.isalpha()).map(lambda x: (x, 1)) \
 .reduceByKey(lambda a,b: a+b)
sqlContext.registerDataFrameAsTable(sqlContext.createDataFrame(words, ['word', 'count']), 'words')

## Execute this code in new paragraph
%sql select word, max(count) from words group by word
