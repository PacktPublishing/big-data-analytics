'''
Start Pyspark Command line using standalone master
[root@myhost ~]# pyspark --master spark://sparkmasterhostname:7077 --total-executor-cores 4

or

[root@myhost ~]# pyspark --master local[4]

'''

# Check Default Parallelism
sc.defaultParallelism


#Let’s create a list, parallelize it and let’s check the number of partitions. 
myList = ["big", "data", "analytics", "hadoop" , "spark"]
myRDD = sc.parallelize(myList)
myRDD.getNumPartitions()

#To override the default parallelism, provide specific number of partitions needed while creating the RDD. In this case let’s create the RDD with 6 partitions.
myRDDWithMorePartitions = sc.parallelize(myList,6)
myRDDWithMorePartitions.getNumPartitions()
 
#Let’s issue an action to count the number of elements in the list.
myRDD.count()

#Display the data in each partition

myRDD.mapPartitionsWithIndex(lambda index,iterator: ((index, list(iterator)),)).collect()

#Increase number of partitions and display contents
mySixPartitionsRDD = myRDD.repartition(6)
mySixPartitionsRDD.mapPartitionsWithIndex(lambda index,iterator: ((index, list(iterator)),)).collect()

#Decrease number of partitions and display contents
myTwoPartitionsRDD = mySixPartitionsRDD.coalesce(2)
myTwoPartitionsRDD.mapPartitionsWithIndex(lambda index,iterator: ((index, list(iterator)),)).collect()

# Check Lineage Graph
print myTwoPartitionsRDD.toDebugString()

