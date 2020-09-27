#!/usr/bin/python3

import sys

current_index=None
current_value=0
good=0
for record in sys.stdin:

	record = record.strip()
	index, contribution = record.split('\t',1)
	try:
		contribution=float(contribution)
	except ValueError:
		continue
	
	if current_index==index:
		if contribution == 0:
			good=1
		current_value+=contribution
	else:
		if current_index and good==1:
			total = 0.15 + (0.85*current_value)
			total = round(total,5)
			print('%s,%s'%(current_index,total))
			good=0
		current_index=index
		current_value=contribution
		if contribution == 0:
			good=1
if current_index==index and good==1:
	total = 0.15 + (0.85*current_value)
	total = round(total,5)
	print('%s,%s'%(current_index,total))
