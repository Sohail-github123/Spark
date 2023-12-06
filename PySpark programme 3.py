# in python
# Create an RDD of tuples (name,age)

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import count

if __name__ =="_main_" :
    if len(sys.argv) !=2:
        print("usage: mnmcount <file>", file=sys.stderr)
        sys.exit(-1)

spark = (SparkSession
         .builder
         .appName("PythonMnMCount")
         .getOrCreate())

dataRDD = sc.parallelize([("Brook",20),("Denny",31),("Jules",30),
                          ("TD",35),("Brooke",25)])
agesRDD = (dataRDD
    .map(lambda x: (x[0],(x[1],1)))
    .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y [1]))
    .map(lambda x: (x[0],x[1][0]/x[1][1])))