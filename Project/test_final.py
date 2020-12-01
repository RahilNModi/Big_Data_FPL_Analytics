from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext 
from pyspark.sql.functions import lit
from pyspark.sql import SparkSession

import sys
import json



configuration = SparkConf()
configuration.setAppName('Project')


spark_context = SparkContext(conf=configuration)
spark = SparkSession(spark_context)
streaming_context = StreamingContext(spark_context,2)


streaming_context.checkpoint('Project Checkpoint')
input_stream = streaming_context.socketTextStream('localhost',6100)




##########################################################



sql = SQLContext(spark_context)

players_df = sql.read.format('csv').option("header", "true").load("file:////home/himanshu/BigData/Assignment/Final/BD_FPL/Code/Data/players.csv")

teams_df = sql.read.format('csv').option("header", "true").load("file:////home/himanshu/BigData/Assignment/Final/BD_FPL/Code/Data/teams.csv")

players_df = players_df.withColumn("PassAccuracy" , lit(0))
players_df = players_df.withColumn("DualEffectiveness" , lit(0))
players_df = players_df.withColumn("FreeKicks" , lit(0))
players_df = players_df.withColumn("TargetShots" , lit(0))
players_df = players_df.withColumn("FoulLoss" , lit(0))
players_df = players_df.withColumn("OwnGoal" , lit(0))




##########################################################





### Has to be modified
final = {'passAcc':{'accNor':0 , 'accKey':0, 'Nor':0, 'Key':0, 'value':0}, 'dualEffect':{'won':0 , 'neutral':0 , 'total':0, 'value':-1}, 'freeKicks':{'goal':0 , 'penalty':0 , 'total':0}, 'targetShots':{'tarGoal':0 , 'tarNotGoal':0 , 'total':0, 'value':0}, 'foulLoss':{'foul':0}, 'ownGoal':{'goal':0},'contribution':0 , 'rating' :0.5 ,'minutesPlayed' :0}




### 1) Pass Accuracy
def pass_accuracy_calculation(x,records):
	playerId = x[0]
	tags = x[1][0]
	sub_event = x[1][1]
	
	for i in tags:
		
		## Norm Pass
		if sub_event == 85:
			records['passAcc']['Nor'] += 1	
			
			## Accurate Normal Pass
			if i["id"] ==  1801:
				records['passAcc']['accNor'] += 1		
		
		## Key Pass
		if i["id"] ==  302:
			records['passAcc']['Key'] += 1
			
			## Accurate Key Pass
			if i["id"] ==  1801:
				records['passAcc']['accKey'] += 1
				
	try:
		records['passAcc']['value'] = (records['passAcc']['accNor'] + (records['passAcc']['accKey']*2))/(records['passAcc']['Nor'] + (records['passAcc']['Key'] * 2))
		
	except ZeroDivisionError:
		pass
	
	
	return {playerId:records}
	
	
	
	
	
### 2) Duel Effectiveness
def dualEffect_calculation(x, records):
	
	playerId = x[0]

	for i in x[1]:
		if(i['id'] == 702):
			records['dualEffect']['neutral']+=1
		elif (i['id'] == 703):
			records['dualEffect']['won']+=1
	
	records['dualEffect']['total']+=1

	records['dualEffect']['value'] = (records['dualEffect']['won'] + (records['dualEffect']['neutral']*0.5))/(records['dualEffect']['total'])
	
	return {playerId:records}

	











### 3) Free Kick Effectiveness
def free_kick_numbers(x,records):
	id_val = x[0]
	tags = x[1][0]
	sub_eve = x[1][1]

	if len(tags) == 0:
		records['freeKicks']['ineffective']+=1
		return {id_val:records}


	for tag in tags:
		if tag['id'] == 1801:
			records['freeKicks']['effective']+=1

		elif tag['id'] == 1802:
			records['freeKicks']['ineffective']+=1

		elif tag['id']==101 and sub_eve == 35:
			records['freeKicks']['penalty']+=1


	try :
		nr = records['freeKicks']['effective']+records['freeKicks']['penalty']
		dr = nr + records['freeKicks']['ineffective']
		records['freeKicks']['effectiveness'] = float(nr)/dr

	except ZeroDivisionError:
		records['freeKicks']['effectiveness'] = 0
		
		
	return {id_val:records}
	
	
	
	
	
	
### 4) Shots on Target	
def shots_on_target_calculation(x, records):
	playerId = x[0]
	tags = x[1][0]
	
	for i in tags:
		## Shots on target and goals
		if i["id"] == 1801 and i["id"] == 101:
			records['targetShots']['tarGoal'] += 1	
		
		## Shots on target and not goals (Doubt)
		if i["id"] == 1801:
			records['targetShots']['tarNotGoal'] += 1
			
	records['targetShots']['total'] += 1
	
	try:
		records['targetShots']['value'] = (records['targetShots']['tarGoal'] + (records['targetShots']['tarNotGoal']*0.5))/(records['targetShots']['total'])
		
	except ZeroDivisionError:
		pass
	
	
	return {playerId:records}
	
	
	
### 5) Foul Loss	
def foulLoss_calculation(x, records):

	records['foulLoss']['foul']+=1
	return {x:records}	
	
	
	
	
	
### Read RDD	
def readstream(rdd):

	if not rdd.isEmpty():
		
		try:
			match_data = rdd.filter(lambda x : 'wyId' in x.keys())
			events_data = rdd.filter(lambda x : 'matchId' in x.keys())
		
			
			teamsinfo = match_data.map(lambda x: x['teamsData'])
			
			subinfo = teamsinfo.flatMap(lambda x :[x[team]['formation']['substitutions'] for team in x.keys()])
			subinfo1 = subinfo.flatMap(lambda x: [(item['playerIn'],1,item['minute']) for item in x])
			subinfo2 = subinfo.flatMap(lambda x: [(item['playerOut'],2,item['minute']) for item in x])
			subinfo = subinfo1.union(subinfo2)
			
			lineupinfo = teamsinfo.flatMap(lambda x :[x[team]['formation']['lineup'] for team in x.keys()])			
			lineupinfo = lineupinfo.flatMap(lambda x: [(item['playerId'],0,0) for item in x])
			
			teamsinfo = subinfo.union(lineupinfo)
			
			teamsinfo1 = teamsinfo.map(lambda x: (x[0],(x[1],x[2]))).reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1]))
			
			
			localdict = dict()
			
			for item in teamsinfo1.collect():
				localdict[item[0]] = {'passAcc':{'accNor':0 , 'accKey':0, 'Nor':0, 'Key':0, 'value':0}, 'dualEffect':{'won':0 , 'neutral':0 , 'total':0, 'value':-1}, 'freeKicks':{'effective':0,'ineffective':0,'penalty':0,'effectiveness':0}, 'targetShots':{'tarGoal':0 , 'tarNotGoal':0 , 'total':0, 'value':0}, 'foulLoss':{'foul':0}, 'ownGoal':{'goal':0},'contribution':0 , 'rating' :0.5 ,'subst' : item[1][0], 'minutesPlayed' :item[1][1]}
			

			
			
			
			###############################################################
			
			### Chemistry of players
			
			
			chemistry = dict()
			'''
			playersID = teamsinfo.map(lambda x : x[0])
			for player in playersID.collect():
				chemistry[player]= dict()
				for i in playersID.collect():
					chemistry[player][i] = 0.5
			'''   
			#print(chemistry)
			#players = teamsinfo1.map(lambda x: Row(playerId = x[0], accNor =0,accKey=0,won=0,neutral=0,totaldualEffect=0,goal=0,penalty=0,totalfreeKicks=0,tarGoal=0,tarNotGoal=0,totaltargetShots=0,foul=0,owngoal=0,contribution=0,rating=0.5,subst = x[1][0],minutesPlayed=x[1][1]))
			#print(players.collect())
			
			#print(playersdf.count())
			
			###############################################################
			
			
			
			
			
			### 1) PASS ACCURACY
			
			pass_events = events_data.filter(lambda x: x["eventId"] == 8 and x['playerId'] != 0)
			pass_accuracy_per_player = pass_events.map(lambda x: (x['playerId'], [x['tags'], x['subEventId']]))
			pass_accuracy_per_event = pass_accuracy_per_player.map(lambda x : pass_accuracy_calculation(x,localdict[x[0]]))
			
			for v in pass_accuracy_per_event.collect():
				localdict.update(v)
				
			
			
			
			### 2) Duel Effectiveness
			
			dual_effectiveness = events_data.filter(lambda y: y['eventId']==1 and y['playerId']!=0)

			dualEffect_for_player = dual_effectiveness.map(lambda x: (x['playerId'], x['tags']))
	
			dual_effectiveness_per_event = dualEffect_for_player.map(lambda x: dualEffect_calculation(x, localdict[x[0]]))
		
			for d in dual_effectiveness_per_event.collect():
				localdict.update(d)
					
					
					
			
			### 3) Free Kick Effectiveness
			
			free_kicks_event_data = events_data.filter(lambda data : data['eventId']==3 and data['playerId']!=0)

			per_player_parameter = free_kicks_event_data.map(lambda data :  (data['playerId'],[data['tags'],data['subEventId']]))

			free_kick_metric_per_event = per_player_parameter.map(lambda x : free_kick_numbers(x,localdict[x[0]]))


			for v in free_kick_metric_per_event.collect():
				localdict.update(v)


			
			
			
			
			### 4) Shots on Target
			
			shots_events = events_data.filter(lambda x: x["eventId"] == 10 and x['playerId'] != 0)
			shots_per_player = shots_events.map(lambda x: (x['playerId'], [x['tags']]))
			shots_per_event = shots_per_player.map(lambda x : shots_on_target_calculation(x, localdict[x[0]]))
			
			for v in shots_per_event.collect():
				localdict.update(v)
				
				
			
			### 5) Foul Loss
			
			events_for_foul_loss = events_data.filter(lambda y: y['eventId']==2 and y['playerId']!=0)
			foulLoss_for_player = events_for_foul_loss.map(lambda x: x['playerId'])
			foulLoss_for_event = foulLoss_for_player.map(lambda x: foulLoss_calculation(x, localdict[x]))
			
			for f in foulLoss_for_event.collect():
				localdict.update(f)
			
			
			### 6) Own Goal

			#self_goal_calculations

			self_goal_events = events_data.filter(lambda data : data['eventId'] == 102 and data['playerId'] != 0)

			for self_goal_player in self_goal_events.collect():

				localdict[self_goal_player['playerId']]['ownGoal']['goal']+=1




			print(localdict)
		
		
		
		except KeyError:
			pass
		

#######################################################





####  Main Point of Start
if input_stream.count():

	convert = input_stream.map(lambda x : json.loads(x)) #convert raw input json to dict

	match = convert.foreachRDD(lambda rdd : readstream(rdd)) #for all rdds in this Dstream goto readstream
	


			
			

#######################################################

streaming_context.start()

streaming_context.awaitTermination()

streaming_context.stop()
