from pyspark.sql import SparkSession
# import pandas as pd

if __name__ == '__main__':

        spark = SparkSession.builder.appName('join').getOrCreate()

        # data1 = spark.read.format('csv').load('/home/sohail/Desktop/New Folder/employees.csv',header=True,inferschema=True)
        # data1.show()
        # data1 = pd.read_csv('/home/sohail/Desktop/New Folder/employees.csv')
        df = spark.read.format('csv')\
        .option('header','true')\
        .option('inferSchema','true')\
        .load('/home/sohail/Desktop/New Folder/employees.csv')

        dept = [("Finance", 10), \
                ("Marketing", 20), \
                ("Sales", 40), \
                ("CLERK", 50), \
                ("PROG", 60), \
                ("Manager", 70), \
                ("frontend", 80), \
                ("Backend", 90), \
                ("FI_ACCOUNT", 100), \
                ("dataengineer", 110) \
                ]
        deptcolumns = ['DEPARTMENT_NAME','deptDEPARTMENT_ID']
        deptDF = spark.createDataFrame(dept,deptcolumns)
        deptDF.show()

        df.join(deptDF, df.DEPARTMENT_ID == deptDF.deptDEPARTMENT_ID, 'inner') \
                .show(truncate=False)




