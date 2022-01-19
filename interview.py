from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()

# Read directory of Http common log data
df = spark.read.text("data.txt")

# Parse log for timestamp (down to the minute) + HTTP Code
df = df.withColumn("log_array", split(df.value, " "))
df = df.withColumn("HttpCode", df.log_array.getItem(8).alias("HTTPcode"))
df = df.withColumn("time", substring_index(df.log_array.getItem(3), ":", 3))
df = df.withColumn("time", df.time.substr(-17, 17))

# Grouped counts of HTTP Codes per min
http_code_counts_per_min = df.groupBy([df.time, df.HttpCode]).count().orderBy(df.time, df.HttpCode)


use_pre_defined_http_code_list = False
#use_pre_defined_http_code_list = True

# Do we care about all HTTP codes in our results or just a select list? A select list avoids the distinct query
tracked_http_codes = []
if use_pre_defined_http_code_list:
    tracked_http_codes = ["200", "404", "500"]
    http_code_counts_per_min = http_code_counts_per_min.where(col("HttpCode").isin(tracked_http_codes))
else:
    distinct_codes = http_code_counts_per_min.select("HttpCode").distinct().orderBy(http_code_counts_per_min.HttpCode)
    tracked_http_codes = distinct_codes.select("HttpCode").rdd.flatMap(lambda x: x).collect()

# Add HTTP code columns to the DF with their respective counts, to match output format
for code in tracked_http_codes:
    http_code_counts_per_min = http_code_counts_per_min.withColumn(code, when(http_code_counts_per_min.HttpCode == lit(code), 1).otherwise(0) * col("count"))

http_code_counts_per_min = http_code_counts_per_min.drop("count").drop("HttpCode")
# aggregate to a single minute row, summing the counts of each code
output = http_code_counts_per_min.groupBy(http_code_counts_per_min.time).sum()

for code in tracked_http_codes:
    output = output.withColumnRenamed( "sum({})".format(code), code )

output_path = "results.csv"
output.write.format('com.databricks.spark.csv').mode('overwrite').option("header", "true").save(output_path)

