from pyspark.sql import SparkSession
from pyspark.sql.functions import avg,min,max

spark=SparkSession\
    .builder.\
        appName('alias').\
            getOrCreate()

data=[(1,'Salman','M',2000,'IT'),
      (2,'Aamir','M',3000,'IT'),
      (3,'kat','F',4000,'HR'),
      (4,'Jack','M',5000,'Payroll'),
      (5,'kareena','F',5000,'HR'),
      (6,'Sohail','M',5000,'IT'),
      (7,'amjad','M',5000,'Payroll')]

schema=['id','name','gender','salary','dep']

df=spark.createDataFrame(data,schema)
print('simple dataframe')
df.show()

print('data groupBy dep ')
df.groupBy('dep').count().show()

print('groupBy dep,gender ')
df.groupBy('dep','gender').count().show()

print('groupBy minimum salary ')
df.groupBy('dep').min('salary').show()

print('roupBy maximum salary')
df.groupBy('dep').max('salary').show()

print('average of salary')
df.groupBy('dep').avg('salary').show()

spark.stop()