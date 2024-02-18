from pyspark.sql import SparkSession
from pyspark.sql.functions import filter

spark=SparkSession\
    .builder.\
        appName('alias').\
            getOrCreate()

data=[(1,'Salman','male',2000),
      (2,'Aamir','male',3000),
      (3,'kat','female',4000),
      (4,'Sharukh','male',5000)]

schema=['id','name','gender','salary']

df=spark.createDataFrame(data,schema)
df.show()

print('use of filter function')
df.filter(df.gender == 'male').show()

print('use of where function')
df.where(df.gender == 'female').show()

df.where((df.gender == 'male') & (df.gender == 'female')).show(truncate=False) 

print('multiple use of where functions')
df.where((df.gender == 'male') & (df.salary == 3000)).show()