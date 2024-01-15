from pyspark.sql import SparkSession

if __name__ == '__main__':

    spark = SparkSession.builder.appName('dataframe2').getOrCreate()

    data1 = [(1,'sohail',25),(2,'aamir',25),(3,'khan',12),(4,'wakar_sir',35)]

    columns = ["col1","col2","col3" ]

    df1 = spark.createDataFrame(data1, columns)

    data2 = [(1,'sohail','akola'),(2,'aamir','pune'),(3,'khan','america'),(4,'wakar_sir','shicago')]

    columns = ["col1","col2","col3"]

    df2 = spark.createDataFrame(data2, columns)

    df1.join(df2, (df1.col1 == df2.col1)).show()

# from pyspark.sql import *
#
# if __name__ == '__main__':
#     print('hello')
#
#     spark = SparkSession.builder \
#         .appName('hellospark') \
#         .master("local[*]") \
#         .getOrCreate()
#
#     data1 = [(1,"sohail","M",25),
#                  (2,"ahmad","M",26),
#                  (3,"shaikh","M",27)]
#
#     data2 = [(1,"sohail","M","Akola"),
#                  (2,"ahmad","M","Muzaffarnagar"),
#                  (3,"shaikh","M",'maharashtra')]
#
#     columns = [("col1","col2","col3","value")]
#
# #    data1 = [('a', 1, "x", 10), ("b", 2, "y", 20), ("c", 3, "z", 30)]
# #    data2 = [("a", 1, "x", "apple"), ("b", 2, "y", "banana"), ("d", 4, "w", "watermelon")]
#
#   #  columns = [("col1", "col2", "col3", "value")]
#
#     df1 = spark.createDataFrame(data1, columns)
#     df2 = spark.createDataFrame(data2, columns)
#
#     joined_df = df1.join(df2, on=["col1", "col2"])
#
#     output_path = "/home/sohail/Desktop/to/save/diabetes.csv"

   # joined_df.write.csv(output_path, header=True, mode='overwrite')

    # df = spark.createDataFrame(data_list).toDF('Id',"Name", "Age")
    # df.show()


