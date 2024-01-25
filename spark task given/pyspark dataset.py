from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .appName("Spark") \
    .master("local[*]") \
    .getOrCreate()

data = [(1,"Quantum Innovations",12.5,14.5,16.5,18.5,20.5),
        (2,"Stellar Solutions",14.5,16.5,18.5,20.5,22.5),
        (3,"Nebulla Dynamics",16.5,18.5,20.5,22.5,24.5),
        (4,"Fusion Enterprises",18.5,20.5,22.5,24.5,26.5),
        (5,"Celestial Technologies",20.5,22.5,24.5,26.5,28.5)
        ]

schema = ["company_id","company_name","2024-02-01","2024-02-02","2024-02-03","2024-02-04","2024-02-05"]

dataDF = spark.createDataFrame(data=data, schema=schema)
dataDF.show()

ids_columns = ["company_id","company_name"]

value_columns = [col for col in dataDF.columns if col not in ids_columns]
print(value_columns)

dataunpivot=dataDF.unpivot("company_name",value_columns,"Date","Stock_price")
dataunpivot.show()


