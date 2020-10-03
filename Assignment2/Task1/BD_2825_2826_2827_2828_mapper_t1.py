#!/usr/bin/python3

import sys

for records in sys.stdin:			#streaming input

	records = records.strip()	#preprocess
	if '#' in records:
		continue
	records = records.split('\t',1) #input file is \t seperated
	
	if len(records)==2:

		print('%s\t%s' % (records[0],records[1]))
