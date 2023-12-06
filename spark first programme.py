# #import the necessary libararies.
# Since we are useing python, import the SoarkSession and related functions
# from the pyspark module.
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import count

if __name__ =="_main_" :
    if len(sys.argv) !=2:
        print("usage: mnmcount <file>", file=sys.stderr)
        sys.exit(-1)

    # Build a SparkSession using the
    # if SparkSessionAPIs is None:
    # If one does not exist, then create an instance. There
    # can only be one SparkSession per jvm.

    spark = (SparkSession
         .builder
         .appName("PythonMnMCount")
         .getOrCreate())

#Get the M&M data set filename from the command-line arguments
    mnm_file = sys.argv[1]
# Read the file into a Spark DataFrame using the CSV
# format by inferring the schema and specifying that the
# file contains a header, which provides column names for comma-
# seprated fields.
    mnm_df = (spark.read.format("csv")
        .option("header","true")
        .option("interSchema","true")
        .load(mnm_file))

# We use the DataFrame high-level APIs.Note
# that we don't use RDDs at all. Because some of Spark's
# function return the same object we can chain function call
# 1. Select from the Dataframe the fields "State","Color"and "Count"
# 2.Since we want to group each state and its M&M clor count,
# we use groupBy()
# 3. Aggregate counts of all colors and groupBy() State and Color
# 4. orderBy() in descending order

    count_mnm_df = (mnm_df
                .select("State","Color","Count")
                .groupBy("State","Color")
                .age(count("Count").alias("Total"))
                .orderBy("Total",ascending=False))
# Show the resulting aggregations for all the state and colors;
# a total count of each color per state.
# Note show() is an action, which will trigger the above
# query to be executed.

    count_mnm_df.show(n=60, truncate=False)
    print("Total Rows = %d" % (count_mnm_df.count()))

########
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