from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import min, max, lit,row_number,count,col,when

spark = SparkSession \
    .builder \
    .appName('LNTinterviewquestion') \
    .master('local[*]') \
    .getOrCreate()

# Get the highest and lowest salary employee in each department if salary is same 
# then return name of employee whose name is come first in lexicographical order.

emp_data=[('captainamerica',1,3000),('ironman',2,5000),
          ('spiderman',1,2000),('Thor',2,4000),('hulk',1,1000)]
emp_schema=['emp_name','dep_id','salary']
df=spark.createDataFrame(emp_data,emp_schema)
df.show()

df1=df.withColumn('row_n',row_number().over(Window.partitionBy(col('dep_id')).orderBy(col('salary'),col('emp_name'))))
df1.show()

df1=df1.withColumn('count',count(lit(1)).over(Window.partitionBy(col('dep_id'))))
df1.show()

# finding the required output using the when()
df_output=df1.groupBy(col('dep_id')).agg(
    max(when(col('row_n') == 1, col('emp_name'))).alias('emp_min_salary'),
                                        max(when(col('row_n') == col('count'),col('emp_name'))).alias('emp_max_salary'))
df_output.show()