from pyspark.sql import SparkSession, functions as func

spark = (
    SparkSession.builder.master("local").appName("Google_PlayStore_Data").getOrCreate()
)

df = spark.read.csv("./googleplaystore.csv", header=True, inferSchema=True)

df.printSchema()
print("count is " + str(df.count()))
df.show(10)

# For top 10 reviews given to the apps

topReviews = (
    df.withColumn("Reviews", func.col("Reviews").cast("int"))
    .groupBy("App")
    .agg(func.sum("Reviews").alias("Top_Reviews"))
    .orderBy(func.desc("Top_Reviews"))
    .limit(10)
)

topReviews.show()

topInstalledApps = (
    df.withColumn("Installs", func.regexp_replace(func.col("Installs"), "[^0-9]", ""))
    .withColumn("Installs", func.col("Installs").cast("int"))
    .groupBy("App")
    .agg(func.sum("Installs").alias("Top_Install_Apps"))
    .orderBy(func.desc("Top_Install_Apps"))
    .limit(10)
)

topInstalledApps.show()

# Category-wise distribution of installed Apps

categoryWiseDistribution = df.select("Category").distinct()

categoryWiseDistribution.show()

#Top paid Apps

topPaidApps = (
    df.filter(func.col("Type") == "Paid")
    .withColumn("Installs", func.regexp_replace(func.col("Installs"), "[^0-9]", ""))
    .withColumn("Installs", func.col("Installs").cast("int"))
    .groupBy("App", "Type")
    .agg(func.sum("Installs").alias("Top_Install_Apps"))
    .orderBy(func.desc("Top_Install_Apps"))
    .limit(10)
)

topPaidApps.show()


# Top paid rating apps

topPaidRatingApps = (
    df.filter(func.col("Type") == "Paid")
    .withColumn("Rating", func.col("Rating").cast("int"))
    .groupBy("App", "Type")
    .agg(func.avg("Rating").alias("Avg_Rating"))
    .orderBy(func.desc("Avg_Rating"))
    .limit(10)
)

topPaidRatingApps.show()
