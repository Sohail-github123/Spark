from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, explode ,expr
from pyspark.sql.types import *

spark = SparkSession.builder.appName("DataFrameCreation").getOrCreate()

# Sample Data (Note: StringType is generally good enough for dates)
schema = StructType([
    StructField("id", IntegerType()), 
    StructField("start_date", TimestampType()), 
    StructField("end_date", TimestampType()),
    StructField("data", StringType())
])

data = [
    (6696902, "2024-02-02 10:30:00", "2024-03-02 18:17:00", "xfbxcvbx"),
    (8535098, "2024-02-02 09:30:00", "2024-03-02 18:17:00", "xfbxcvbx"),
    (8858051, "2024-02-02 19:30:00", "2024-02-02 22:30:00", "xfbxcvbx") 
]

# Create the DataFrame
df = spark.createDataFrame(data)
df1 = df.withColumnRenamed("_1","id").withColumnRenamed("_2","start_date").withColumnRenamed("_3","end_date").withColumnRenamed("_4","data")

df2 = df1.withColumn("start_date", to_timestamp(col("start_date"))) \
.withColumn("end_date", to_timestamp(col("end_date")))

result = df2.withColumn("timestamps", expr("sequence(start_date, end_date, interval 1 hour)")) \
.select("id", explode("timestamps").alias("hourly_basis"), "data")
result.show()