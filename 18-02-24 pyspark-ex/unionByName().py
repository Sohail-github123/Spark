
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc,asc,cast,like

spark=SparkSession\
    .builder.\
        appName('alias').\
            getOrCreate()

data=[(1,'Salman',2000)]

schema=['id','name','salary']

data1=[(5,'Salman',25)]

schema1=['id','name','age']

df=spark.createDataFrame(data,schema)
df.show()

df1=spark.createDataFrame(data1,schema1)
df1.show()

newdf=df.unionByName(df1 ,allowMissingColumns=True).show()

# newdf1=df.union(df1)
# newdf1.distinct().show()

# union() and unionAll() both are work same . These both are give duplicates values.
# if you want to remove duplicates values then use this distinct()  with union() and union All() .
