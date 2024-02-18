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

df=spark.createDataFrame(data,schema)
df.show()
print('SIMPLE_DATA_FRAME')
df.printSchema()
print('SIMPLE_DATA_FRAME')

df1=df.select(df.id.alias('emp_id'),df.name.alias('emp_name'),df.salary.alias('emp_salary'))
df1.show()
print('ALIAS_DATAFRAME')

df2=df.sort(df.name.desc())
df2.show()
print('DESC_order_DATAFRAME')

df3=df.sort(df.name.asc())
df3.show()
print('ASC_order_DATAFRAME')

df4=df.filter(df.name.like('S%')).show()
print('use_of_LIKE()_in_DATA_FRAME')

df5=df.select(df.salary.cast('int'),df.id.cast('int'))
df5.show()
df5.printSchema()

print('use_of_cast()_in_DATA_FRAME')