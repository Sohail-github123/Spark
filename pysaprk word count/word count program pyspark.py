from pyspark.sql import SparkSession

from operator import add
import sys
import time

if __name__ == "__main__":
    print("Apache spark -word count")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))

    if len(sys.argv) == 1:
        program_name = sys.argv[0]
        print("program_name: " + program_name)
    else:
        print("Failed, no input arguments.")
        exit(1)

    spark = SparkSession \
        .builder \
        .appName("Apache spark count") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    yarn_log_file_path = "/home/sohail/PycharmProjects/pythonProject/.venv/test data fro word count"
    lines_rdd = spark.sparkContext.textFile(yarn_log_file_path)

    lines_list = lines_rdd.collect()
    print(lines_list)

    words_count_rdd = lines_rdd.flatMap(lambda line: line.split(',')) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(add)
    result = words_count_rdd.collect()
    print(result)
    print(type(result))
    for (word, count) in result :
     print("%s:  %i"%(word,count))
    print("Apache spark word count end here")
