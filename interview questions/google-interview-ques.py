from pyspark.sql import SparkSession
from pyspark.sql.functions import split,explode,col,lit,count,desc,count
spark = SparkSession \
.builder \
.appName('googleinterviewquestion') \
.master('local[*]') \
.getOrCreate()

data=[('pythonbootcamp1.txt','python the data analytics 0 to hero bootcamp staring june 6th'),
      ('pythonbootcamp2.txt','class will be held on weekends from 11am to 1pm for 5-6 weeks'),
      ('pythonbootcamp3.txt','use code NY2024 to get 33percents off.you can registre from namaste sql websites.link is planned comments  ')]

schema=['filename','content']

df= spark.createDataFrame(data=data,schema=schema)
df.show(truncate=False)

df= df.select('content', split(df.content,' ').alias('array_of_words'))
df.show(truncate=False)

df= df.select('content',explode(col("array_of_words")).alias('words'))
df.show(truncate=False)

df= df.groupby('words').agg(count(lit(1)).alias('count_of_words'))
df.show(truncate=False)

df=df.filter(col("count_of_words")>1).sort(col("count_of_words").desc())
df.show(truncate=False)

