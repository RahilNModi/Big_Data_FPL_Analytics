import sys
import json 


player_profile_handle = open('Data/final_player_profile.json','r')

	
player_profile_data = json.loads(player_profile_handle.read())


match_profile_handle = open('Data/final_match_data.txt','r')


match_records = match_profile_handle.readlines()

matches_dict = dict()

for match in match_records:

	match_data = eval(match)

	matches_dict.update(match_data)

match_profile_handle.close()

file = sys.argv[1]
	
print("\n"*3)

#path-1 prediction
#path-2	player-profile
#path-3 match-data

f_chemistry = open('Data/final_chemistry.txt','r')

chemistry = eval(f_chemistry.read())

f_chemistry.close()

def match_predictor(query):

	teamId1 = query['team1']['name']
	teamId2 = query['team2']['name']
	roles = {'DF':0,'GK':0,'MD':0 ,'FW':0}
	localdict = dict()
	for playeri in query['team1'].values():
		for playerid,value in player_profile_data.items():
				if value['name']==playeri:
					roles[value['role']] = roles.get(value['role']) +1
					value['team'] = teamId1
					localdict[value['playerId']] = value
					break
	if not (roles['GK'] == 1 and roles['DF'] >=3 and roles['MD']>=2 and roles['FW'] >=1):
		return "Invalid Team"
	
	roles = {'DF':0,'GK':0,'MD':0 ,'FW':0}
	for playeri in query['team2'].values():
		for playerid,value in player_profile_data.items():
				if value['name']==playeri:
					roles[value['role']] = roles.get(value['role']) +1
					value['team'] = teamId2
					localdict[value['playerId']] = value
					break
	if not (roles['GK'] == 1 and roles['DF'] >=3 and roles['MD']>=2 and roles['FW'] >=1):
		return "Invalid Team"
		
		
	team1 = []
	team2 = []
	#print(localdict)
	for i in localdict.keys():

		# Avg chm coeff
		
		player_id_chem = [ chemistry[(p,q)] for p,q in list(chemistry.keys()) if p == i and p!=q and localdict.get(q,0)!=0 and localdict[q]['team'] == localdict[p]['team']]
		if len(player_id_chem)==0:
			continue
		player_id_chem_avg = sum(player_id_chem) / len(player_id_chem)
		#avg_chem_of_all_players[i] = player_id_chem_avg

		# Rating
		try:
			player_rating = localdict[i]['rating']
		except:
			player_rating = 0.5
		## if player < match -> Clustering
		#print(player_rating)
		# Player Strength
		player_strength = player_id_chem_avg * player_rating
		#print(teamId[0][0],teamId[0][1])
		if localdict[i]['team'] == teamId1:
		    team1.append(player_strength)
		    
		else:
		    team2.append(player_strength)
		

	#print(team1 , team2)
	## Team Strength
	try:
		strength_A = sum(team1) / len(team1)
		strength_B = sum(team2) / len(team2)
	except:
		strength_A =0
		strength_B =0

	## Winning Chance
	#print(strength_A, strength_B)
	## Chance of A winning
	winning_chance_A = (0.5 + (strength_A - ((strength_A + strength_B) / 2))) * 100
	winning_chance_B = 100 - winning_chance_A

	res = {"A":winning_chance_A,"B":winning_chance_B}
	#print("here", strength_A,strength_B)
	return (res)
			

def player_retrieve(query):

	print("\n\n\nREQUESTING FOR PLAYER PROFILE OF PLAYER : ",query['name'],"\n\n\n")

	for player_ids in player_profile_data:

		if player_profile_data[player_ids]['name'] == query['name']:

			total_keys = set(player_profile_data[player_ids].keys())-set(['no_matches','rating','playerId','passportArea'])

			player_profile_data[player_ids]['passacc'] = round(player_profile_data[player_ids]['passacc']*100)
			player_profile_data[player_ids]['targetshots'] = round(player_profile_data[player_ids]['targetshots']*100)

			if player_profile_data[player_ids]['no_matches']!=0:

				res = dict((k,player_profile_data[player_ids][k]) for k in  total_keys )

				return res
			else:

				player_profile_data[player_ids]['passacc']=0
				player_profile_data[player_ids]['targetshots']=0
				player_profile_data[player_ids]['goals']=0
				player_profile_data[player_ids]['foul']=0
				player_profile_data[player_ids]['owngoal']=0
				res = dict((k,player_profile_data[player_ids][k]) for k in  total_keys )

				return res
				
	return "INVALID PLAYER"


def match_retrieve(query):

	print("\n\n\nREQUESTING FOR MATCH DATA ",query['date'],"  ",query['label'])

	key_1 = query['date']

	key_2 = query['label'].split(',')

	key_2 = [k.strip() for k in key_2]

	key_20= key_2[0].split('-',2)

	team_a=key_20[0].strip()

	team_b=key_20[1].strip()

	score_a,score_b = key_2[1].split('-',2)

	score_a = int(score_a)

	score_b = int(score_b)

	key_2 = (team_a,team_b,score_a,score_b)

	return matches_dict.get((key_1,key_2))





print(" "*60,"FPL ANALYTICS QUERY UI"," "*50)


query_file = open(file,'r')

query = query_file.read()

query = eval(query)
#print(query)
if 'req_type' in query.keys():  #prediction or player_profile
	
		
	if query['req_type']==2:

		result = player_retrieve(query)

	else:

		result = match_predictor(query)


	print(result,"\n\n\n")


else:

	match_result = match_retrieve(query)




	print("\n"*3,match_result,"\n"*3)





query_file.close()

player_profile_handle.close()
