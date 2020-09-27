#! /usr/bin/python3

import sys

file_path = sys.argv[-1] #get the file loc

file_handler = open(file_path,'r')

for transitions in sys.stdin:

	source , destinations = transitions.strip().split('\t',1)

	destinations = eval(destinations)

	entry_in_v = file_handler.readline()

	if entry_in_v:

		entry_in_v = entry_in_v.strip().split(',')

		page_rank_v = entry_in_v[1]

	else :

		break

	try :

		page_rank_v = float(page_rank_v)

	except ValueError:

		continue


	
	total_nodes = len(destinations)

	for dest_node in destinations:

		contribution_by_v_to_dest_node = round(page_rank_v/total_nodes,5)

		print(dest_node,'\t',contribution_by_v_to_dest_node)


file_handler.close()







