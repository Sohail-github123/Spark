#in python

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

#Create a DataFrame using SparkSession
spark = (SparkSession
    .builder
    .appName("AuthorsAges")
    .getOrCreate())

#Create a DataFrame
data_df = spark.createDataFrame([("Brook",20),("Denny",31),("Jules",30),
                                ("TD",35),("Brook",25)],["name","age"])

#Group the same names together, aggregate their ages, and compute an average
avg_df = data_df.groupBy("name").agg(avg("age"))

#Show the results of the final execution
avg_df.show()