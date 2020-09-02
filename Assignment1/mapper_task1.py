#!/usr/bin/env python3

import sys
import json
import datetime

target = sys.argv[1]
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
