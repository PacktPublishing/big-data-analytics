//Below Scala example creates a Dataset and DataFrame from an RDD. Enter to scala shell with //spark-shell command. 

case class Dept(dept_id: Int, dept_name: String)

val deptRDD = sc.makeRDD(Seq(Dept(1,"Sales"),Dept(2,"HR")))

val deptDS = spark.createDataset(deptRDD)

val deptDF = spark.createDataFrame(deptRDD)

//Notice that when you convert Dataset to RDD, you get RDD[Dept]. But, when you convert //DataFrame to RDD, you get RDD[Row]

deptDS.rdd

//res12: org.apache.spark.rdd.RDD[Dept] = MapPartitionsRDD[5] at rdd at 
//<console>:31

deptDF.rdd

//res13: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = //MapPartitionsRDD[8] at rdd at <console>:31


//Compile time safety check is done as shown in below code. Since dept_location is not a //member of Dept case class, it will throw an error.

deptDS.filter(x => x.dept_location > 1).show()

//<console>:31: error: value dept_location is not a member of Dept
//       deptDS.filter(x => x.dept_location > 1).show()
