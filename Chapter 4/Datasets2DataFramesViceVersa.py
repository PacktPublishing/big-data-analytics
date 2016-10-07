//A DataFrame can be converted to a Dataset by providing a class with “as” method as // shown in below example.

val newDeptDS = deptDF.as[Dept]

//newDeptDS: org.apache.spark.sql.Dataset[Dept] = [dept_id: int, dept_name: string]

//Use toDF function to convert Dataset to DataFrame.  Here is another Scala example 
//for converting the Dataset created above to DataFrame. 

newDeptDS.first()

//res27: Dept = Dept(1,Sales)

newDeptDS.toDF.first()

//res28: org.apache.spark.sql.Row = [1,Sales]

//Note that res27 is resulting a Dept case class object and res28 is resulting a Row object.

