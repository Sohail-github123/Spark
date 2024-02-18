from pyspark.sql import SparkSession
from pyspark.sql.functions import filter

spark=SparkSession\
    .builder.\
        appName('orderBy').\
            getOrCreate()

data=[(1,'Salman','M',2000,'IT'),
      (2,'Aamir','M',3000,'HR'),
      (3,'kat','F',4000,'HR'),
      (4,'Sharukh','M',5000,'Payroll')]

schema=['id','name','gender','salary','dep']

df=spark.createDataFrame(data,schema)
df.show()
# df.sort(df.dep).show()
#df.sort('dep','salary').show()
# df.sort('dep').show()
#df.sort(df.dep.desc(),df.salary.desc()).show()
df.orderBy(df.dep).show()
df.orderBy('dep','salary').show()
df.orderBy('dep').show()
df.sort(df.dep.desc(),df.salary.desc()).show()