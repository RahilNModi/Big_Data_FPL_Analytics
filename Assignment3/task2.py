import pyspark
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType
import time 

target_word = sys.argv[1]
target_count = sys.argv[2]

# join based on key_id
# word == target_word
# recognized == False and Total_Strokes < target_count
# print(countrycode, total_stroke_of_that_country)

spark = SparkSession.builder.appName('A3T2').getOrCreate()

df1 = spark.read.option('header',True).format("csv").load(sys.argv[3])

df1.dropDuplicates()
target_only_df1 = df1.filter((col('word') == target_word)).drop("word")

# To be decided
if target_only_df1.rdd.isEmpty():
	print('0')
	quit()
	
	
df2 = spark.read.option('header',True).format("csv").load(sys.argv[4])

df2 = df2.withColumn("Total_Strokes", df2["Total_Strokes"].cast(IntegerType())) 
df2.dropDuplicates()
target_only_df2 = df2.filter((col('word') == target_word))


# JOIN
df = target_only_df1.join(target_only_df2, on=["key_id"], how="inner")


# FILTER
df = df.filter((col('recognized') == False))
df = df.filter((col('Total_Strokes') < int(target_count)))


final_dict = {}
result = df.groupBy('countrycode').count().collect()

for row in result:
	if row["countrycode"] not in final_dict:
		final_dict[row["countrycode"]] = row["count"]
		
		
for key in sorted(final_dict.keys()):
    print(str(key).strip()+","+str(final_dict[key]).strip())
