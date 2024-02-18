from pyspark.sql import SparkSession
from pyspark.sql.functions import * 

spark=SparkSession\
    .builder.\
        appName('alias').\
            getOrCreate()

data=[(1,'Salman','male',2000),
      (2,'Aamir','male',3000),
      (2,'Aamir','male',3000),
      (3,'katrina','female',4000),
      (4,'Sharukh','male',5000)]

schema=['id','name','gender','salary']

df=spark.createDataFrame(data,schema)
df.show()

df.distinct().show()
