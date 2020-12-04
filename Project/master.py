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
streaming_context = StreamingContext(spark_context,4)


streaming_context.checkpoint('Project Checkpoint')
input_stream = streaming_context.socketTextStream('localhost',6100)




##########################################################

chemistry = dict()

sql = SQLContext(spark_context)

##########################################################

## Player Profile


player_profile = {} 
      
    # Open a csv reader called DictReader 
with open("/home/himanshu/BigData/Assignment/Final/BD_FPL/Code/Data/players.csv", encoding='utf-8') as csv: 
          
        # Convert each row into a dictionary  
        # and add it to data 
        for rows in csv:
        	l = rows.split(",")
        	try:
        		player_profile[int(l[8].strip('\n'))]= {'name' : l[0] , 'birtharea' : l[1] , 'birthdate':l[2] ,'foot': l[3] ,'role':l[4] ,'height':l[5],'passportArea':l[6],'Weight':l[7], 'foul':0,'goals':0,'owngoal':0,'passacc':0,'targetshots':0,'playerId':int(l[8].strip('\n')),'rating' : 0.5, 'no_matches':0}
        	except:
        		pass
              


teams = {}
with open("/home/himanshu/BigData/Assignment/Final/BD_FPL/Code/Data/teams.csv", encoding='utf-8') as csvf: 
          
        # Convert each row into a dictionary  
        # and add it to data 
        for rows in csvf:
        	l = rows.split(",")
        	try:
        		teams[int(l[1].strip('\n'))] = l[0]
        	except:
        		pass


##########################################################




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


def funcin(x):
	l = list()
	for item in x:
		for i in item[0]:
			try:
				l.append((i['playerIn'],1,i['minute'],item[1]))
			except:
				continue
	return l	
	
	
def funcout(x):
	l = list()
	for item in x:
		for i in item[0]:
			try:
				l.append((i['playerOut'],2,i['minute'],item[1]))
			except:
				continue
	return l
	
	
	
	
	
### Read RDD	
def readstream(rdd):

	if not rdd.isEmpty():
		
		try:
			
			global player_profile
			match_data = rdd.filter(lambda x : 'wyId' in x.keys())
			
			if match_data.isEmpty():
				return 
			match_details = dict()
			match = match_data.collect()[0]
			teamId = list(match['teamsData'].keys())
			key = (match['dateutc'].split(' ')[0], (teams[int(teamId[0])].strip(' FC'), teams[int(teamId[1])].strip(' FC'), match['teamsData'][teamId[0]]['score']+match['teamsData'][teamId[0]]['scoreP'], match['teamsData'][teamId[1]]['score']+match['teamsData'][teamId[1]]['scoreP']))

			match_details[key] = dict()

			match_details[key]['date'] = match['dateutc'].split(' ')[0]
			match_details[key]['duration'] = match['duration']
			match_details[key]['winner'] = match['winner']
			match_details[key]['venue'] = match['venue']
			match_details[key]['gameweek'] = match['gameweek']
			match_details[key]['goals'] = list()
			match_details[key]['own_goals'] = list()
			match_details[key]['yellow_cards'] = list()
			match_details[key]['red_cards'] = list()

			events_data = rdd.filter(lambda x : 'matchId' in x.keys())
		
			
			teamsinfo = match_data.map(lambda x: x['teamsData'])
			
			subinfo = teamsinfo.map(lambda x :[(x[team]['formation']['substitutions'],team) for team in x.keys()])
			subinfo1 = subinfo.flatMap(lambda x: funcin(x))
			subinfo2 = subinfo.flatMap(lambda x: funcout(x))
			subinfo = subinfo1.union(subinfo2)

			teamId = teamsinfo.map(lambda x: [team for team in x.keys()]).collect()
			
			
			lineupinfo = teamsinfo.map(lambda x :[(x[team]['formation']['lineup'],team) for team in x.keys()])

			for i in lineupinfo.collect()[0]:
				for j in i[0]:
					if(int(j['goals'])>0):
						d = dict()
						d['name'] = player_profile[int(j['playerId'])]['name']
						d['team'] = teams[int(i[1])].strip(' FC')
						d['number_of_goals'] =	int(j['goals'])	
						match_details[key]['goals'].append(d)
					if(int(j['ownGoals'])>0):
						d = dict()
						d['name'] = player_profile[int(j['playerId'])]['name']
						d['team'] = teams[int(i[1])].strip(' FC')
						d['number_of_goals'] =	int(j['ownGoals'])	
						match_details[key]['own_goals'].append(d)
					if(int(j['redCards'])>0):
						match_details[key]['red_cards'].append(player_profile[int(j['playerId'])]['name'])
					if(int(j['yellowCards'])>0):
						match_details[key]['yellow_cards'].append(player_profile[int(j['playerId'])]['name'])

			lineupinfo = lineupinfo.flatMap(lambda x: [(i['playerId'],0,0,item[1]) for item in x for i in item[0]])
			
			teamsinfo = subinfo.union(lineupinfo)
			teamsinfo1 = teamsinfo.map(lambda x: (x[0],(x[1],x[2],x[3]))).reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1],x[2]))
			
			###############################################################
			
			
			
			localdict = dict()
			
			
			
			for item in teamsinfo1.collect():
				localdict[item[0]] = {'passAcc':{'accNor':0 , 'accKey':0, 'Nor':0, 'Key':0, 'value':0}, 'dualEffect':{'won':0 , 'neutral':0 , 'total':0, 'value':-1}, 'freeKicks':{'effective':0,'ineffective':0,'penalty':0,'effectiveness':0}, 'targetShots':{'tarGoal':0 , 'tarNotGoal':0 , 'total':0, 'value':0}, 'foulLoss':{'foul':0}, 'ownGoal':{'goal':0},'contribution':0 , 'rating' :0.5 ,'subst' : item[1][0], 'minutesPlayed' :item[1][1], 'team' : item[1][2], 'changeinRating' :0}

			

			
			###############################################################
			
			
			
			
			### 1) PASS ACCURACY
			
			pass_events = events_data.filter(lambda x: x["eventId"] == 8 and x['playerId'] != 0 and x['playerId'] in localdict.keys())
			pass_accuracy_per_player = pass_events.map(lambda x: (x['playerId'], [x['tags'], x['subEventId']]))
			pass_accuracy_per_event = pass_accuracy_per_player.map(lambda x : pass_accuracy_calculation(x,localdict[x[0]]))
			
			for v in pass_accuracy_per_event.collect():
				localdict.update(v)
				
			
			
			
			### 2) Duel Effectiveness
			
			dual_effectiveness = events_data.filter(lambda y: y['eventId']==1 and y['playerId']!=0 and y['playerId'] in localdict.keys())

			dualEffect_for_player = dual_effectiveness.map(lambda x: (x['playerId'], x['tags']))
	
			dual_effectiveness_per_event = dualEffect_for_player.map(lambda x: dualEffect_calculation(x, localdict[x[0]]))
		
			for d in dual_effectiveness_per_event.collect():
				localdict.update(d)
					
					
					
			
			### 3) Free Kick Effectiveness
			
			free_kicks_event_data = events_data.filter(lambda data : data['eventId']==3 and data['playerId']!=0 and data['playerId'] in localdict.keys())

			per_player_parameter = free_kicks_event_data.map(lambda data :  (data['playerId'],[data['tags'],data['subEventId']]))

			free_kick_metric_per_event = per_player_parameter.map(lambda x : free_kick_numbers(x,localdict[x[0]]))


			for v in free_kick_metric_per_event.collect():
				localdict.update(v)


			
			
			
			
			### 4) Shots on Target
			
			shots_events = events_data.filter(lambda x: x["eventId"] == 10 and x['playerId'] != 0 and x['playerId'] in localdict.keys())
			shots_per_player = shots_events.map(lambda x: (x['playerId'], [x['tags']]))
			shots_per_event = shots_per_player.map(lambda x : shots_on_target_calculation(x, localdict[x[0]]))
			
			for v in shots_per_event.collect():
				localdict.update(v)
				
				
			
			### 5) Foul Loss
			
			events_for_foul_loss = events_data.filter(lambda y: y['eventId']==2 and y['playerId']!=0 and y['playerId'] in localdict.keys())
			foulLoss_for_player = events_for_foul_loss.map(lambda x: x['playerId'])
			foulLoss_for_event = foulLoss_for_player.map(lambda x: foulLoss_calculation(x, localdict[x]))
			
			for f in foulLoss_for_event.collect():
				localdict.update(f)
			
			
			### 6) Own Goal

			#self_goal_calculations

			self_goal_events = events_data.filter(lambda data : data['eventId'] == 102 and data['playerId'] != 0 and data['palyerId'] in localdict.keys())

			for self_goal_player in self_goal_events.collect():

				localdict[self_goal_player['playerId']]['ownGoal']['goal']+=1




			
			
			
			######  END OF THE MATCH
			
			
			
			
			## For each player
			for i in localdict.keys():
			
				### Player Contribution
				
				
				p_a = localdict[i]['passAcc']['value']
				d_e = localdict[i]['dualEffect']['value']
				fk_e = localdict[i]['freeKicks']['effectiveness']			
				s_t = localdict[i]['targetShots']['value']
				
				player_contribution = (p_a + d_e + fk_e + s_t) / 4
				
				
				## Normalize
				
				## never substituted
				if localdict[i]['subst'] == 0:
					player_contribution = player_contribution * 1.05
					
				## substituted
				else:
					player_contribution = player_contribution * (localdict[i]['minutesPlayed'] / 90)
					
					
				
				localdict[i]['contribution'] = player_contribution
				
				
				
				### Player Performance
				player_performance = player_contribution \
										- localdict[i]['foulLoss']['foul'] * (0.5 / 100) \
										- localdict[i]['ownGoal']['goal'] * (5 / 100)
										
				
				### New Player Rating
				temp = (player_performance + localdict[i]['rating']) / 2
				localdict[i]['changeinRating'] = temp - localdict[i]['rating'] 
				localdict[i]['rating'] = temp

				rec = player_profile[i]
				rec['foul'] = rec['foul']+localdict[i]['foulLoss']['foul']
				rec['goals'] = rec['goals']+localdict[i]['targetShots']['tarGoal']
				rec['owngoal'] = rec['owngoal']+localdict[i]['ownGoal']['goal']
				if(rec['passacc']==0):
					rec['passacc'] = localdict[i]['passAcc']['value']
				else:
					rec['passacc'] = (rec['passacc']+localdict[i]['passAcc']['value'])/2
				if(rec['targetshots']==0):
					rec['targetshots'] = localdict[i]['targetShots']['value']
				else:
					rec['targetshots'] = (rec['targetshots']+localdict[i]['targetShots']['value'])/2
				rec['no_matches'] = rec['no_matches'] + 1
				rec['rating'] = temp
				player_profile.update({i:rec})
		
			### Chemistry
			for index,player in enumerate(list(localdict.keys())):
				for i in list(localdict.keys())[index:]:
					newaverage = abs((localdict[player]['changeinRating'] + localdict[i]['changeinRating'])/2)
					if localdict[player]['team'] ==  localdict[i]['team']:
				        	
				        	if (localdict[player]['changeinRating'] >0 and  localdict[i]['changeinRating']>0) or (localdict[player]['changeinRating'] >0 and  localdict[i]['changeinRating']>0):
				        		newchange = (0.5 if chemistry.get((player,i),0)==0 else chemistry[(player,i)]) + newaverage
				        	else :
				        		newchange = (0.5 if chemistry.get((player,i),0)==0 else chemistry[(player,i)]) - newaverage
				        		
					else:
				        	if (localdict[player]['changeinRating'] >0 and  localdict[i]['changeinRating']>0) or (localdict[player]['changeinRating'] >0 and  localdict[i]['changeinRating']>0):
				        		newchange = (0.5 if chemistry.get((player,i),0)==0 else chemistry[(player,i)]) - newaverage
				        	else:
				        		newchange = (0.5 if chemistry.get((player,i),0)==0 else chemistry[(player,i)]) + newaverage

					chemistry[(player,i)] = newchange

			##################################################################
			
			### Predict Chances of Winning
			
          
			team1 = []
			team2 = []
		    
			for i in localdict.keys():

				# Avg chm coeff

				player_id_chem = [ chemistry[(p,q)] for p,q in list(chemistry.keys()) if p == i and localdict[q]['team'] == localdict[p]['team']]
				if len(player_id_chem)==0:
					continue
				player_id_chem_avg = sum(player_id_chem) / len(player_id_chem)
				#avg_chem_of_all_players[i] = player_id_chem_avg

				# Rating
				player_rating = player_profile[i]['rating']

				## if player < match -> Clustering

				# Player Strength
				player_strength = player_id_chem_avg * player_rating
				#print(teamId[0][0],teamId[0][1])
				if localdict[i]['team'] == teamId[0][0]:
				    team1.append(player_strength)
				    
				else:
				    team2.append(player_strength)


			## Team Strength
			try:
				strength_A = sum(team1) / len(team1)
				strength_B = sum(team2) / len(team2)
			except:
				strength_A =0
				strength_B =0

			## Winning Chance

			## Chance of A winning
			winning_chance_A = (0.5 + (strength_A - ((strength_A + strength_B) / 2))) * 100
			winning_chance_B = 100 - winning_chance_A

			res = {"A":winning_chance_A,"B":winning_chance_B}
			#print(res)
			
			print(localdict)
			print("*"*150)
			print(player_profile)
			print("*"*150)
			print(match_details)
			print("*"*150)
			print(chemistry)
			

		except:
			pass
		

#######################################################

def empty():
	pass



####  Main Point of Start
if input_stream.count():

	convert = input_stream.map(lambda x : json.loads(x)) #convert raw input json to dict

	match = convert.foreachRDD(lambda rdd : empty() if rdd.count()==0 else  readstream(rdd)) #for all rdds in this Dstream goto readstream
			
			
'''
## Store in hdfs
print("Writing into HDFS")

p_p = json.dumps(player_profile)
spark.read.json(spark_context.parallelize([p_p])).coalesce(1).write.mode('append').json('/data/profile')
'''


#######################################################

streaming_context.start()

streaming_context.awaitTermination()

streaming_context.stop()
