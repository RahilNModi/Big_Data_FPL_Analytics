#!/usr/bin/python3

import sys

path = sys.argv[1]
f = open(path,'r')

for records in sys.stdin:	#streaming input
	records = records.strip()	#preprocess
	index, destlist = records.split('\t',1) #input file is \t seperated
	destlist = destlist.strip()
	reslist = destlist.strip('][').split(', ')
	line=f.readline()
	if not line:
		break	
	indexinv,rank =line.strip().split(',',1)
	try:
		rank=float(rank)
	except ValueError:
		continue
	for dest in reslist:
		try:
			dest=int(dest)
		except ValueError:
			continue
		prob = float(1/len(reslist))
		contribution = round(prob*rank,4)
		print('%s\t%s' % (dest,contribution))
	

