from pyspark.sql import SparkSession
if __name__ == '__main__':
    spark = SparkSession.builder.appName('name').getOrCreate()

    df = spark.read.format('csv') \
        .option('header', 'true') \
        .option('inferSchema', 'true') \
        .load('/home/sohail/Downloads/loan.csv')

    df1 = spark.read.format('csv') \
        .option('header', 'true') \
        .option('inferSchema', 'true') \
        .load('/home/sohail/Downloads/borrower.csv')

    df.join(df1, (df.LOAN_NO == df1.LOAN_NO)&(df.PAN_NUMBER==df.PAN_NUMBER)&(df.AADHAR==df1.AADHAR),'outer') \
         .show(truncate=False)




















# from pyspark.sql import SparkSession
#
# if __name__ == '__main__':
#
#         spark = SparkSession.builder.appName('join').getOrCreate()
#
#         df1 = spark.createDataFrame(
#             [(1,'a',2.0),(2,'b',3.0),(3,'c',3.0)],
#             ('x1','x2','x3'))
#
#         df2 = spark.createDataFrame(
#             [
#                 (1,'f',1.0),(2,'b',0.0)],('x1','x2','x3'))
#         df = df1.join(df2, ['x1','x2'])
#
#         df.show()




# from pyspark.sql import SparkSession
#
# if __name__ == '__main__':
#
#         spark = SparkSession.builder.appName('join').getOrCreate()
#
#         df1 = spark.createDataFrame(
#             [(10,'a',2.0),(20,'b',3.0),(30,'c',3.0), \
#              (10,'a',2.0),(20,'b',3.0),(30,'c',3.0),\
#              (10,'a',2.0),(20,'b',3.0),(30,'c',3.0)],\
#             ('x1','x2','x3'))
#
#         df2 = spark.createDataFrame(
#             [(10,'f',1.0),(20,'b',0.0),(30,'v',3.2),\
#              (10,'f',1.0),(20,'b',0.0),(30,'v',3.2),\
#              (10,'f',1.0),(20,'b',0.0),(30,'v',3.2)],\
#             ('x1','x2','x3'))
#
#         df = df1.join (df2, (df1.x1==df2.x1) & (df1.x2== df2.x2) & (df1.x3==df2.x3))
#         df.show(2)



