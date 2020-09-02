import sys
import json
import datetime

target = sys.argv[1]	# target -> word (airplane)

# Initialization
print ('%s\t%s' % (target, "0"))
print ('%s\t%s' % ("Weekend", "0"))

for item in sys.stdin:
	row = json.loads(item)
	 
	if (
	row["word"] == target and 		# word
	
	all(x.isalpha() or x.isspace() for x in row["word"]) and 	# alphabets and white spaces			
	
	type(row["recognized"]) == bool and		# recognized
	
	len(row["drawing"]) > 0 and all(len(stroke)==2 for stroke in row["drawing"]) 
	# n(>=1) strokes and every stroke has exactly 2 arrays
	
	):
	
		if (
		len(row["countrycode"])==2 and row["countrycode"].isupper() and 	# countrycode
		
		len(row["key_id"])==16 and row["key_id"].isnumeric()		# key_id (numeric and 16 digits)
		):

			if row["recognized"]:
				print ('%s\t%s' % (row["word"], "1"))
				
			else:
				time_day = datetime.datetime.strptime(row["timestamp"], '%Y-%m-%d %H:%M:%S.%f %Z').weekday()
				if time_day == 5 or time_day == 6:
					print('%s\t%s' % ("Weekend", "1"))	
	
