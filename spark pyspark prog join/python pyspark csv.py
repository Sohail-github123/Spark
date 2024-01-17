import pyspark
from pyspark.sql import SparkSession

if __name__ == '__main__':

    spark = SparkSession.builder.appName('sohail').getOrCreate()

    emp = [(1,"sohail",-1,"2019","10","m",3000), \
           (2,"aamir",1,"2023","20","m",4000), \
           (3,"ahmad",1,"2022","10","m",3000),\
           (4,"arshan",2,"2020","40","m",3000),\
           (5,"abuzar",2,"2018","60","",-2),\
           (6,"khan",2,"2024","10","",-1),\
           ]
    empcolumns = ['emp_id','name','superior_emp_id',"year_joined", \
                  "emp_dept_id","gender","salary"]

    empDF = spark.createDataFrame(data=emp,schema = empcolumns)
    # empDF.printSchema()
    # empDF.show(truncate=False)

    dept = [("Finance",10), \
            ("Marketing",20), \
            ("Sales",30), \
            ("IT",40)\
            ]
    deptColumns = ["dept_name","dept_id"]
    deptDF = spark.createDataFrame(data=dept, schema=deptColumns)
    # deptDF.printSchema()
    # deptDF.show(truncate=False)

    empDF.join(deptDF,empDF.emp_dept_id == deptDF.dept_id,'inner') \
    .show(truncate=False)

    #empDF.write.mode('overwrite').csv('/home/sohail/Desktop/new')