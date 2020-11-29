from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext 
import sys
import json

configuration = SparkConf()
configuration.setAppName('Project')

spark_context = SparkContext('local[2]',conf=configuration)
streaming_context = StreamingContext(spark_context,2)

streaming_context.checkpoint('Project Checkpoint')

input_stream = streaming_context.socketTextStream('localhost',6100)


##########################################################

#Read the csv files in to dframes

#PROFILE SECTION#

#this readstream deals with one whole match and its related events
#just put some characters to see how the stream of data comes in .
#Run and verify
def readstream(rdd):

	if not rdd.isEmpty():
		print("*"*100)  # asterisk denotes beginning of new batch (match) 
		for recs in rdd.collect(): #rdd.collect will have dictionaries , first dictionary is match data. Rest all are event data

			print("!"*100) # exclaim mark denotes beginning of a record (match data/event data), basically it is a dict

			try:

				print(recs['matchId']) #only first record will not have this , because 'matchId' is present in  events data

			except KeyError:

				print(recs['wyId']) #only first record will have this , because 'wyId' is present in match data only


############################################################



sql = SQLContext(spark_context)

players_df = sql.read.format('csv').option("header", "true").load("path/BD_FPL/BD_FPL/Data/play.csv")

teams_df = sql.read.format('csv').option("header", "true").load("path/BD_FPL/BD_FPL/Data/teams.csv")

players_df.show(10)


teams_df.show(10)

##3#######################################################

if input_stream.count():

	convert = input_stream.map(lambda x : json.loads(x)) #convert raw input json to dict

	match = convert.foreachRDD(lambda rdd : readstream(rdd)) #for all rdds in this Dstream goto readstream
	


			
			

#######################################################

streaming_context.start()

streaming_context.awaitTermination()

streaming_context.stop()



