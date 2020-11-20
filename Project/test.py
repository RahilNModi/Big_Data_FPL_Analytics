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


sql = SQLContext(spark_context)

players_df = sql.read.format('csv').option("header", "true").load("path/BD_FPL/BD_FPL/Data/play.csv")

teams_df = sql.read.format('csv').option("header", "true").load("path/BD_FPL/BD_FPL/Data/teams.csv")

players_df.show(10)


teams_df.show(10)

##3#######################################################
  
	
input_stream.pprint()

			
			

#######################################################

streaming_context.start()

streaming_context.awaitTermination()

streaming_context.stop()



