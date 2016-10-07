import sys
from operator import  add
from pyspark import SparkContext

# Check for number of inputs passed from command line
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print >> sys.stderr, "Usage: access_log.py <file>"
        exit(-1)

# Intialize the Spark Context with app name
sc = SparkContext(appName="Log Analytics")

# Get the lines from the textfile, create 4 partitions
access_log = sc.textFile(sys.argv[1], 4)

#Filter Lines with ERROR only
error_log = access_log.filter(lambda x: "ERROR" in x)

# Cache error log in memory
cached_log = error_log.cache()

# Now perform an action -  count
print “Total number of error records are %s” % (cached_log.count())

# Now find the number of lines with 
print “Number of product pages visited that have Errors is %s” % (cached_log.filter(lambda x: “product” in x).count()) 
