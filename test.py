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

players_df = sql.read.format('csv').option("header", "true").load("/home/rahil/Hadoop/Examples/Project/players.csv")

teams_df = sql.read.format('csv').option("header", "true").load("/home/rahil/Hadoop/Examples/Project/players.csv")

players_df.show(10)


teams_df.show(10)

play = players_df.groupBy("Id").count().collect()
#print(new) Row(Id='346135', count=1)
players = dict()
final = {'passAcc':{'accNor':0 , 'accKey':0, 'value':-1}, 'dualEffect':{'won':0 , 'neutral':0 , 'total':0, 'value':-1}, 'freeKicks':{'goal':0 , 'penalty':0 , 'total':0, 'value':-1}, 'targetShots':{'tarGoal':0 , 'tarNotGoal':0 , 'total':0, 'value':-1}, 'foulLoss':{'foul':0, 'value':-1},'contribution':0 , 'rating' :0.5 ,'minutesPlayed' :0}
#chemistry = dict()
#count =0
for row in play:
	if players.get(row['Id'],0)==0:
		#count +=1
		players[row['Id']] = final
		#chemistry[row['Id']] = 0.5
'''		
for key, value in players.items():
	players[key]['chemistry'] = chemistry		
'''
team = teams_df.groupBy("Id").count().collect()
#print(new) Row(Id='346135', count=1)
teams = dict()
for row in team:
	if teams.get(row['Id'],0)==0:
		teams[row['Id']] = [0,0,0,0,0,0]

#print(players)
#print(count)
with open('player.txt' ,'w') as f:
	json.dump(players , f)
	

with open('team.txt' ,'w') as f:
	json.dump(teams , f)

print(players)
print(teams)

##3#######################################################

def dualEffect_calculation(x):
	playerId = str(x['playerId'])
	for i in x['tags']:
		if(i['id'] == 702):
			players[playerId]['dualEffect']['neutral']+=1
		elif (i['id'] == 703):
			players[playerId]['dualEffect']['won']+=1
	players[playerId]['dualEffect']['total']+=1
	players[playerId]['dualEffect']['value'] = (players[playerId]['dualEffect']['won'] + (players[playerId]['dualEffect']['neutral']*0.5))/(players[playerId]['dualEffect']['total'])
	return players[playerId]['dualEffect']['value']
  
def foulLoss_calculation(x):
	players[str(x['playerId'])]['foulLoss']['foul']+=1
	return players[str(x['playerId'])]['foulLoss']['foul']
	
if input_stream.count():
	input_stream.pprint()
	'''
	dstream = input_stream.map( lambda x: json.loads(x))
	events_for_dual_effectiveness = dstream.filter(lambda y: 'eventId' in y and y['eventId']==1 )
	#events_for_dual_effectiveness.pprint()

	dualEffect_for_player = events_for_dual_effectiveness.filter(lambda x: x['playerId']!=0).map(lambda x: dualEffect_calculation(x))
	#dualEffect_for_player.pprint()

	events_for_foul_loss = dstream.filter(lambda y: 'eventId' in y and y['eventId']==2)
	events_for_foul_loss.pprint()

	foulLoss_for_player = events_for_foul_loss.filter(lambda x: x['playerId']!=0).map(lambda x: foulLoss_calculation(x))
	foulLoss_for_player.pprint()
	'''

	

			

#######################################################

streaming_context.start()

streaming_context.awaitTermination()

streaming_context.stop()
