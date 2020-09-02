#!/usr/bin/env python3

import sys
import json
import datetime

target = sys.argv[1]
print ('%s\t%s' % (target, "0"))
print ('%s\t%s' % ("Weekend", "0"))
for item in sys.stdin:
	row = json.loads(item)
	if row["word"]==target and all(x.isalpha() or x.isspace() for x in row["word"]) and type(row["recognized"])==bool and 		all(len(y)==2 for y in row["drawing"]):
		if len(row["countrycode"])==2 and row["countrycode"].isupper() and len(row["key_id"])==16 and row["key_id"].isnumeric():
			if row["recognized"]:
				print ('%s\t%s' % (row["word"], "1"))
			else:
				time_day = datetime.datetime.strptime(row["timestamp"], '%Y-%m-%d %H:%M:%S.%f %Z').weekday()
				if time_day == 5 or time_day == 6:
					print('%s\t%s' % ("Weekend", "1"))	

#data = [json.loads(str(item)) for item in sys.stdin] # List of dict of values
#print(data[0])
'''
word = "airplane"

if all(x.isalpha() or x.isspace() for x in word): # Check word
	
	for i in data:
		if i["recognized"]:
			print ('%s\t%s' % (word, "1"))
		
		else:
			time_day = datetime.datetime.strptime(i["timestamp"], '%Y-%m-%d %H:%M:%S.%f %Z').weekday()
			
			if time_day == 5:
				print('%s\t%s' % ("Saturday", "1"))
				
			if time_day == 6:
				print('%s\t%s' % ("Sunday", "1"))
			
			
'''




'''

unique_word = {}

for i in data:
	if i["word"] not in unique_word:
		unique_word[i["word"]] = 1
	else:
		unique_word[i["word"]]+=1
		
print(unique_word)

{'airplane': 10}
{'airplane': 151623, 'aircraft carrier': 116503, '@irplane': 1}

'''
