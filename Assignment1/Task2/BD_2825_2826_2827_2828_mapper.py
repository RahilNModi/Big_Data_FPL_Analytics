#!/usr/bin/python3

import sys
import json

#count =10000			#test for first 10000 recs only

target =sys.argv[len(sys.argv)-2]		#target_word read as an input argument

threshold_distance = int(sys.argv[len(sys.argv)-1])	#threshold distance factor 

for recs in sys.stdin :	#read from input stream
	

	pure_dict = json.loads(recs)	#convert string dictionary into a python dict()
	
	curr_word = pure_dict['word']	#get the word attr
	
	if curr_word == target and all(x.isalpha() or x.isspace() for x in pure_dict["word"]) and type(pure_dict["recognized"])==bool and all(len(y)==2 for y in pure_dict["drawing"]):	#perform comparison
		
		if len(pure_dict["countrycode"])==2 and pure_dict["countrycode"].isupper() and len(pure_dict["key_id"])==16 and pure_dict["key_id"].isnumeric():
		
				#print(pure_dict['countrycode'],'  ',1)

				stroke_one = pure_dict['drawing'][0]	#get the first stroke from drawing

				stroke_one_x = stroke_one[0][0]	#x coord from stroke_one list
				#print("Stroke_one_x:",stroke_one_x)
				stroke_one_y = stroke_one[1][0]	#y coord from stroke_one list
				#print("Stroke_one_y:",stroke_one_y)
				#print(stroke_one_x,stroke_one_y,sep="  ,   ")

				if (stroke_one_x**2 + stroke_one_y**2 )**0.5 > threshold_distance :	#condition check , euclidean criteria

					print(pure_dict['countrycode'],',',1,sep="")		#print
	
	
