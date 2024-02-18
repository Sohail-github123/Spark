
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc,asc,cast,like

spark=SparkSession\
    .builder.\
        appName('alias').\
            getOrCreate()

data=[(1,'Salman',2000),
      (2,'Aamir',3000),
      (3,'Sohail',4000),
      (4,'Sharukh',5000)]

schema=['id','name','salary']

data1=[(5,'rahul',1540),
      (6,'katrina',6400),
      (7,'kareena',2000),
      (8,'jack',1000),
      (4,'Sharukh',5000)]

schema1=['id','name','salary']

df=spark.createDataFrame(data,schema)
df.show()

df1=spark.createDataFrame(data1,schema1)
df1.show()

newdf=df.union(df1)
newdf.show()