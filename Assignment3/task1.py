from __future__ import print_function
import sys
import re
from statistics import mean
from pyspark import SparkContext

d = dict()

def generate_key_value(row):
	if(d=={}):
		key = 0
		parts = re.split(r',', row)
		for i in parts:
			d[i] = key
			key = key + 1
	else:
		parts = re.split(r',', row)
		return (parts[d['key_id']], parts[d['word']], parts[d['recognized']], parts[d['Total_Strokes']])
		

if __name__=="__main__":
	if len(sys.argv) != 4:
		print("Usage: Find_average <file> <word> <shape.csv> <shape_stat.csv>", file=sys.stderr)
		sys.exit(-1)
        
	word = sys.argv[1]
        #Initialise spark context
	spark_context = SparkContext("local", "Average app")
        	
        #Read dataset2 as a RDD
	dataset2 = spark_context.textFile(sys.argv[3])

	records = dataset2.map(lambda line: generate_key_value(line)).distinct().filter(lambda x: x!=None)
	
	recog_records = records.filter(lambda x: ('True'==x[2] and word==x[1])).map(lambda x: int(x[3]))
	
	unrecog_records =  records.filter(lambda x: ('False'==x[2] and word==x[1])).map(lambda x: int(x[3]))
	
	if(recog_records.collect()==[]):
		print(0.0)
	else:
		print(round(mean(recog_records.collect()),5))
	if(unrecog_records.collect()==[]):
		print(0.0)
	else:
		print(round(mean(unrecog_records.collect()),5)) 
	
	
	spark_context.stop()
