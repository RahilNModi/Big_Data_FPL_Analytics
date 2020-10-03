#!/usr/bin/python3

import sys

path = sys.argv[1]
f = open(path,'r')
dict_of_source = dict()

for statement in f:
	indexinv,rank = statement.strip().split(',',1)
	rank = float(rank)
	dict_of_source[indexinv] = rank
	print('%s\t%s' % (indexinv,0))
	
	
for records in sys.stdin:	#streaming input
	records = records.strip()	#preprocess
	index, destlist = records.split('\t',1) #input file is \t seperated
	destlist = destlist.strip()
	index= index.strip()
	reslist = eval(destlist) #.strip('][').split(', ')
	#print(reslist)
	for node in reslist:
		if node in dict_of_source.keys():
			print('%s\t%s' % (node, float(dict_of_source[index]/len(reslist))))

