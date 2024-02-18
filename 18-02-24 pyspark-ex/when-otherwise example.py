from pyspark.sql import SparkSession
from pyspark.sql.functions import col,when
spark=SparkSession \
    .builder \
    .appName('when') \
    .getOrCreate()

data=[(1,'Salman','M',2000),
      (2,'Aamir','M',3000),
      (1,'kat','F',4000),
      (1,'abcd','',5000)]
schema=['id','name','gender','salary']

df=spark.createDataFrame(data,schema)
df.show()

df1=df.select(df.id,df.name,when(df.gender=='M','male') 
              .when(df.gender=='F','female') 
              .otherwise('unknown')) 
df1.show()

df.select(df.id,df.name,when(df.gender=='M','male')
          .when(df.gender=='F','female')
          .otherwise('unknown').alias('gender')).show()
