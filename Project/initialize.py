
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext 
import sys
import json

configuration = SparkConf()
configuration.setAppName('Project')

spark_context = SparkContext('local[2]',conf=configuration)

sql = SQLContext(spark_context)

players_df = sql.read.format('csv').option("header", "true").load("/home/sneha/hadoop/Copy of Data/Data/players.csv")

teams_df = sql.read.format('csv').option("header", "true").load("/home/sneha/hadoop/Copy of Data/Data/teams.csv")



play = players_df.groupBy("Id").count().collect()
#print(new) Row(Id='346135', count=1)
players = dict()
final = {'passAcc':{'accNor':0 , 'accKey':0}, 'dualEffect':{'won':0 , 'neutral':0 , 'total':0}, 'freeKicks':{'goal':0 , 'penalty':0 , 'total':0}, 'targetShots':{'tarGoal':0 , 'tarNotGoal':0 , 'total':0}, 'foulLoss':{'foul':0}, 'dualEffect':{'goal':0},'contribution':0 , 'rating' :0.5 ,'minutesPlayed' :0}
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
	

