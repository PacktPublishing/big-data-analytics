"""
Create test table in HBase first:
hbase(main):001:0> create 'test', 'f1'
0 row(s) in 0.7840 seconds

Then, submit jobs as 

spark-submit --master local[*] --driver-class-path /usr/lib/spark/lib/spark-examples.jar hbase_outputformat localhost test row1 f1 q1 value1

spark-submit --master local[*] --driver-class-path /usr/lib/spark/lib/spark-examples.jar hbase_outputformat localhost test row2 f1 q1 value2

Then, Scan table
hbase(main):002:0> scan 'test'
ROW                   COLUMN+CELL
 row1                 column=f1:q1, timestamp=1405659615726, value=value1
 row2                 column=f1:q1, timestamp=1405659626803, value=value2
2 row(s) in 0.0780 seconds
"""


from __future__ import print_function

import sys

from pyspark import SparkContext

if __name__ == "__main__":
    if len(sys.argv) != 7:
        print("""
        Usage: hbase_outputformat <host> <table> <row> <family> <qualifier> <value>
        Run with example jar:
        ./bin/spark-submit --driver-class-path /path/to/example/jar \
        /path/to/examples/hbase_outputformat.py <args>
        Assumes you have created <table> with column family <family> in HBase
        running on <host> already
        """, file=sys.stderr)
        exit(-1)

    host = sys.argv[1]
    table = sys.argv[2]
    sc = SparkContext(appName="HBaseOutputFormat")

    conf = {"hbase.zookeeper.quorum": host,
            "hbase.mapred.outputtable": table,
            "mapreduce.outputformat.class": "org.apache.hadoop.hbase.mapreduce.TableOutputFormat",
            "mapreduce.job.output.key.class": "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
            "mapreduce.job.output.value.class": "org.apache.hadoop.io.Writable"}
    keyConv = "org.apache.spark.examples.pythonconverters.StringToImmutableBytesWritableConverter"
    valueConv = "org.apache.spark.examples.pythonconverters.StringListToPutConverter"

    sc.parallelize([sys.argv[3:]]).map(lambda x: (x[0], x)).saveAsNewAPIHadoopDataset(
        conf=conf,
        keyConverter=keyConv,
        valueConverter=valueConv)

    sc.stop()