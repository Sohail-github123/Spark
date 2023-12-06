import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import count

if __name__ =="_main_" :
    if len(sys.argv) !=2:
        print("usage: mnmcount <file>", file=sys.stderr)
        sys.exit(-1)

    spark = (SparkSession
             .builder
             .appName("PythonMnMCount")
             .getOrCreate())
    mnm_file = sys.argv[1]

    mnm_df = (spark.read.format("csv")
              .option("header", "true")
              .option("interSchema", "true")
              .load(mnm_file))

    count_mnm_df = (mnm_df
                    .select("State", "Color", "Count")
                    .groupBy("State", "Color")
                    .age(count("Count").alias("Total"))
                    .orderBy("Total", ascending=False))

    count_mnm_df.show(n=60, truncate=False)
    print("Total Rows = %d" % (count_mnm_df.count()))

    # while the above code aggregated and counted for all
# the states, ehat if we just want to see the data for
# a single state, e.g., CA?
# 1.Select from all rows in the DataFrame
# 2.Filter only CA state
# 3.groupBy() state and Color as we did above
# 4.Aggregate the counts for each color
# 5.orderBy() in descending order
#Find the aggregate count for California by filtering

    ca_count_mnm_df = (mnm_df
        .select("State","Color","Count")
        .where(mnm_df.state == "CA")
        .groupBy("State","Color")
        .agg(count("Count").alias("Total"))
        .orderBy("Total",ascending=False))

#Show the resulting aggregation for California.
#As above, show() is an action that will trigger the execution of the
#entire computation.

    ca_count_mnm_df.show(n=10, truncate=False)
#Stop the SparkSession
    spark.stop()