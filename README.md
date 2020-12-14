## $5 Tech Unlocked 2021!
[Buy and download this product for only $5 on PacktPub.com](https://www.packtpub.com/)
-----
*The $5 campaign         runs from __December 15th 2020__ to __January 13th 2021.__*

#Big Data Analytics
This is the code repository for [Big Data Analytics](https://www.packtpub.com/big-data-and-business-intelligence/big-data-analytics?utm_source=github&utm_medium=repository&utm_campaign=9781785884696), published by Packt. It contains all the supporting project files necessary to work through the book from start to finish.
##Instructions and Navigations
All of the code is organized into folders. Each folder starts with a number followed by the application name. For example, Chapter02.



The code will look like the following:
```
from pyspark import SparkConf, SparkContext
conf = (SparkConf()
.setMaster("spark://masterhostname:7077")
.setAppName("My Analytical Application")
.set("spark.executor.memory", "2g"))
sc = SparkContext(conf = conf)
```


Practical exercises in this book are demonstrated on virtual machines (VM) from 
Cloudera, Hortonworks, MapR, or prebuilt Spark for Hadoop for getting started 
easily. The same exercises can be run on a bigger cluster as well.
Prerequisites for using virtual machines on your laptop:
*  RAM: 8 GB and above
*  CPU: At least two virtual CPUs
*  The latest VMWare player or Oracle VirtualBox must be installed for Windows or Linux OS
*  Latest Oracle VirtualBox, or VMWare Fusion for Mac
*  Virtualization enabled in BIOS
*  Browser: Chrome 25+, IE 9+, Safari 6+, or Firefox 18+ recommended 
(HDP Sandbox will not run on IE 10)
*  Putty
*  WinScP

The Python and Scala programming languages are used in chapters, with more focus 
on Python. It is assumed that readers have a basic programming background in Java, 
Scala, Python, SQL, or R, with basic Linux experience. Working experience within 
Big Data environments on Hadoop platforms would provide a quick jump start for 
building Spark applications.

##Related Products
* [Securing Hadoop](https://www.packtpub.com/big-data-and-business-intelligence/securing-hadoop?utm_source=github&utm_medium=repository&utm_campaign=9781783285259)

* [Hadoop for Finance Essentials](https://www.packtpub.com/big-data-and-business-intelligence/hadoop-finance-essentials?utm_source=github&utm_medium=repository&utm_campaign=9781784395162)

* [Big Data Forensics – Learning Hadoop Investigations](https://www.packtpub.com/networking-and-servers/big-data-forensics-–-learning-hadoop-investigations?utm_source=github&utm_medium=repository&utm_campaign=9781785288104)
###Suggestions and Feedback
[Click here](https://docs.google.com/forms/d/e/1FAIpQLSe5qwunkGf6PUvzPirPDtuy1Du5Rlzew23UBp2S-P3wB-GcwQ/viewform) if you have any feedback or suggestions.
