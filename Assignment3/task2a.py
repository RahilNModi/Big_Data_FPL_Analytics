from __future__ import print_function
import sys
import re
from operator import add
from pyspark import SparkContext

d1 = dict()
d2 = dict()

def generate_key_value1(row):
	if(d1=={}):
		key = 0
		parts = re.split(r',', row)
		for i in parts:
			d1[i] = key
			key = key + 1
	else:
		parts = re.split(r',', row)
		return (parts[d1['key_id']], (parts[d1['word']], parts[d1['recognized']], parts[d1['Total_Strokes']]))

def generate_key_value2(row):
	if(d2=={}):
		key = 0
		parts = re.split(r',', row)
		for i in parts:
			d2[i] = key
			key = key + 1
	else:
		parts = re.split(r',', row)
		return (parts[d2['key_id']], parts[d2['countrycode']])
		

if __name__=="__main__":
	if len(sys.argv) != 5:
		print("Usage: Find_average <file> <word> <strokes> <shape.csv> <shape_stat.csv>", file=sys.stderr)
		sys.exit(-1)
		
	word = sys.argv[1]
	strokes = int(sys.argv[2])
	
        #Initialise spark context
	spark_context = SparkContext("local", "Join app")
        #Read dataset2 as a RDD
	dataset1 = spark_context.textFile(sys.argv[4])
	records1 = dataset1.map(lambda line: generate_key_value1(line)).distinct().filter(lambda x: x!=None)	
        
        #Read dataset2 as a RDD
	dataset2 = spark_context.textFile(sys.argv[3])
	records2 = dataset2.map(lambda line: generate_key_value2(line)).distinct().filter(lambda x: x!=None)
	
	join_records = records1.join(records2).map(lambda rec: (rec[0], rec[1][0][0], rec[1][0][1], rec[1][0][2], rec[1][1]))
	
	inter_records = join_records.filter(lambda rec: 'False'==rec[2] and word==rec[1] and strokes > int(rec[3])).map(lambda rec: (rec[4], 1))
		

	final_records = inter_records.reduceByKey(add).sortByKey()
	for i in final_records.collect():
		print(i[0],',',i[1], sep="")	

	
	spark_context.stop()	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
