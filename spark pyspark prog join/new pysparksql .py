import pyspark
from pyspark.sql import SparkSession

if __name__ == '__main__':

    spark = SparkSession.builder.appName('sohail').getOrCreate()

    emp = [(1,"sohail",26,"44401","akola","india",3000), \
           (2,"aamir",26,"53200","pune","india",4000), \
           (3,"ahmad",20,"2022","Dubai","UAE",3000),\
           (4,"arshan",24,"10020","Istanbul","turkey",3000),\
           (5,"abuzar",22,"32045","London","England",2000),\
           (6,"khan",21,"524","balapur","",5000),\
           ]
    empcolumns = ['emp_id','name','age',"pincode", \
                  "address","country","salary"]

    empDF = spark.createDataFrame(data=emp,schema = empcolumns)
    # empDF.printSchema()
    empDF.show(truncate=False)

    dept = [(1,101,"44401","india",3000), \
           (2,102,"53200","india",4000), \
           (3,103,"2022","UAE",3000),\
           (4,104,"10020","turkey",3000),\
           (5,105,"32045","England",2000),\
           (6,106,"524","",5000),\
            ]
    deptColumns = ["emp_id","dept_id","pincode","country","salary"]
    deptDF = spark.createDataFrame(data=dept, schema=deptColumns)
    # deptDF.printSchema()
    deptDF.show(truncate=False)

    empDF.join(deptDF,(empDF.emp_id == deptDF.emp_id) & (empDF.pincode == deptDF.pincode) &(empDF.country == deptDF.country) ,'inner') \
    .show(truncate=False)

    empDF.createOrReplaceTempView("EMP")
    deptDF.createOrReplaceTempView("DEPT")

    joinDF = spark.sql("select * from EMP e, DEPT d where e.emp_id == d.emp_id").show(truncate=False)

    joinDF2 = spark.sql("select * from EMP e INNER JOIN DEPT d on e.emp_id == d.emp_id and e.pincode==d.pincode and e.country == d.country") \
        .show(truncate=False)

    #joinDF2.write.mode('overwrite').csv('/home/sohail/Desktop/new')
    deptDF.write.option("header",True)\
    .csv("/home/sohail/Desktop/new")
