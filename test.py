from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext 
from pyspark.sql.functions import lit
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

players_df = sql.read.format('csv').option("header", "true").load("/home/sneha/hadoop/Copy of Data/Data/players.csv")

teams_df = sql.read.format('csv').option("header", "true").load("/home/sneha/hadoop/Copy of Data/Data/teams.csv")

players_df = players_df.withColumn("PassAccuracy" , lit(0))
players_df = players_df.withColumn("DualEffectiveness" , lit(0))
players_df = players_df.withColumn("FreeKicks" , lit(0))
players_df = players_df.withColumn("TargetShots" , lit(0))
players_df = players_df.withColumn("FoulLoss" , lit(0))
players_df = players_df.withColumn("OwnGoal" , lit(0))




#players_df.show(10)
#teams_df.show(10)
'''
tweet=dataStream.map(lambda w:(w.split(';')[0],1))
count=tweet.reduceByKey(lambda x,y:x+y)
count.pprint()'''

fi = input_stream.map(lambda x : json.loads(x))
#fi.pprint()


global players
players = dict()
final = {'passAcc':{'accNor':0 , 'accKey':0}, 'dualEffect':{'won':0 , 'neutral':0 , 'total':0}, 'freeKicks':{'goal':0 , 'penalty':0 , 'total':0}, 'targetShots':{'tarGoal':0 , 'tarNotGoal':0 , 'total':0}, 'foulLoss':{'foul':0}, 'ownGoal':{'goal':0},'contribution':0 , 'rating' :0.5 ,'minutesPlayed' :0}


def func(playerId):
	global players
	player = playerId.collect()
	#print(player)
	for playeri in player:
		if players.get(playeri,0)==0:
			players[playeri] = final

def convert(x):
	return x	

			
matchfilter = fi.filter(lambda x: x.get('wyId',0)!=0)
#matchfilter.pprint()
if matchfilter:
	teamsinfo = matchfilter.map(lambda x: x['teamsData'])
	#teamsinfo.pprint()
	teamsinfo = teamsinfo.flatMap(lambda x :[x[team]['formation']['lineup'] for team in x.keys()])
	#teamsinfo.pprint()
	teamsinfo = teamsinfo.flatMap(lambda x: [item['playerId'] for item in x])
	teamsrdd = teamsinfo.foreachRDD(func)
	#print(teamsrdd)
	'''for row in teamsrdd:
		if players.get(row,0)==0:
			players[row] = final'''
	
	#teamsinfo.pprint()
#print("done")

print(players)
"""
f = open('player.txt' , 'r')
players = json.load(f)
f.close()

getevents = fi.filter(lambda x: x.get('eventId',0) == 1)
#'tags': [{'id': 1801}]
'''
geteventsTAG701 = getevents.filter(lambda x: any(int(i['id'])==701 for i in x['tags']))
getplayerID = geteventsTAG701.map(lambda x : x['playerId'])
#players[getplayerID]['dualEffect']['total'] += 1

for i in getplayerID:
	players[i]['dualEffect']['total'] += 1
'''

geteventsTAG702 = getevents.filter(lambda x: any(int(i['id'])==702 for i in x['tags']))
getplayerID = geteventsTAG702.map(lambda x : x['playerId']).count()
for i in getplayerID:
	players[i['playerId']]['dualEffect']['total'] += i['count']
	players[i['playerId']]['dualEffect']['neutral'] += i['count']

'''
geteventsTAG703 = getevents.filter(lambda x: any(int(i['id'])==703 for i in x['tags']))
getplayerID = geteventsTAG703.map(lambda x : x['playerId']).collect()
for i in getplayerID:
	players[i]['dualEffect']['total'] += 1
	players[i]['dualEffect']['won'] += 1


with open('player.txt' ,'w') as f:
	json.dump(players , f)
'''	
#geteventsTAG701.pprint()

	

try:
	eventnumber = fi['eventId']
	print(eventnumber)
	'''if int(eventnumber) ==1:
		for ID in (tag['id'] for tag in fi['tags']):
			if int(ID)==701:
			
			elif int(ID)==702:
			elif int(ID) == 703: '''
except:
	pass
"""	
'''
with open('player.txt' ,'w') as f:
	json.dump(players , f)
	

with open('team.txt' ,'w') as f:
	json.dump(teams , f)

'''		
##########################################################
  
	
#input_stream.pprint()

			
		

#######################################################

streaming_context.start()

streaming_context.awaitTermination()

streaming_context.stop()
