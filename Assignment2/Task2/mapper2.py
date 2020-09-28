#!/usr/bin/python3

import sys

path = sys.argv[1]
f = open(path,'r')

for records in sys.stdin:	#streaming input
	records = records.strip()	#preprocess
	index, destlist = records.split('\t',1) #input file is \t seperated
	destlist = destlist.strip()
	index= index.strip()
	reslist = eval(destlist) #.strip('][').split(', ')
	#print(reslist)
	print('%s\t%s' % (index,0))
	line=f.readline()
	if not line:
		break	
	indexinv,rank =line.strip().split(',',1)
	try:
		rank=float(rank)
	except ValueError:
		continue
	for dest in reslist:
		prob = float(1/len(reslist))
		contribution = round(prob*rank,5)
		if rank!=0:
			print('%s\t%s' % (dest,contribution))
	

