#!/usr/bin/python3

import sys

for records in sys.stdin:			#streaming input

	records = records.strip()	#preprocess

	records = records.split('\t') #input file is \t seperated

	

	if len(records)==2 and records[0].isnumeric() and records[1].isnumeric():#as the input doesn't guarantee a numeric value for the nodes ,perform a check

		print(records[0],'\t',records[1])
