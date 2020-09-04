#!/usr/bin/python3

import sys

d = {}
for record in sys.stdin:
	record = record.strip()
	label, value = record.split("\t",1)
	try:
		value = int(value)
	except:
		continue
	if label not in d:
		d[label] = value
		if label != "Weekend":
			first_label = label
	else:
		d[label] += value
if d == {}:
	pass
else:
	if d[first_label]:
		print('%s' % (d[first_label]))
	if d["Weekend"]:
		print('%s' % (d["Weekend"]))
