#!/usr/bin/python3

import sys

current_index=None
current_value=0

for record in sys.stdin:
	record = record.strip()
	index, contribution = record.split('\t',1)
	try:
		index=int(index)
		contribution=float(contribution)
	except ValueError:
		continue
	
	if current_index==index:
		current_value+=contribution
	else:
		if current_index:
			total = 0.15 + (0.85*current_value)
			total = round(total,4)
			print('%s,%s'%(current_index,total))
		current_index=index
		current_value=contribution
if current_index==index:
	total = 0.15 + (0.85*current_value)
	total = round(total,4)
	print('%s,%s'%(current_index,total))
