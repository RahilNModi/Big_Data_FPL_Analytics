import pyspark
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType

target=sys.argv[1]
import time 


spark = SparkSession.builder.appName('A3T1').getOrCreate() #create a spark session

df = spark.read.option('header',True).csv(sys.argv[-1])		#read the csv file from hdfs dataset-2, and explicitly request to consider line-1 for #headers
df = df.withColumn("Total_Strokes", df["Total_Strokes"].cast(IntegerType())) #cast to integer , we gotta aggregate this
df.dropDuplicates()		#drop duplicates
target_only = df.filter((col('word')==target))		#select rows with word.value == target_value

if target_only.rdd.isEmpty():
	print('0.00000')
	print('0.00000')
	quit()
							
total_true_false = target_only.groupBy('recognized').count().collect() #groupy by recognised and then find #of true and false entries

#[Row(recognized=u'False', count=17), Row(recognized=u'True', count=182)] : Sample

counts={} #A dictionary with 2 keys only viz True and False , their values are a list which represents the total count of true,strokes_in_true

"""		|--- True ---->[N(recognised=True) , Stroke sum for recognised=True]
	counts --
		|--- False ---->[N(recognised=False) , Stroke sum for recognised=False]
		


"""

for rows in total_true_false:

	if rows['recognized'] not in counts:
		counts[rows['recognized']]  = [rows['count']]










res = target_only.groupBy('recognized').sum('Total_Strokes').collect()
#find the total strokes for recognised == true and then for recognised == false
#[Row(recognized=u'False', sum(Total_Strokes)=149), Row(recognized=u'True', sum(Total_Strokes)=1355)]

for rows in res:
	counts[rows['recognized']].append(rows['sum(Total_Strokes)'])

#by default True is first in the dictionary

for values in counts:
	
	try :
		k = float(counts[values][1])/float(counts[values][0])
		
		print(format(k,"0.5f"))

	except ZeroDivisionError:
		print('0.00000')










