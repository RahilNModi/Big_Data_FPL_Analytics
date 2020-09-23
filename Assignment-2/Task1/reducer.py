#!/usr/bin/python3

import sys

path = sys.argv[1]

current_src_node = None   #Initially no source , dest edge has arrived

dest_list = []			  #contains all the nodes adj to current_src_node

file_handler = open(path,'w')

for edges in sys.stdin:	  #streaming input		

	edges = edges.strip()

	src_node,dest_node = edges.split('\t',1)	#split edges[0] as src_node and edges[1] as dest_node


	try :

		dest_node = int(dest_node)				#convert dest_node to int , because we need to sort the dest_nodes lexically



	except ValueError:

		continue


	if current_src_node == src_node:	#if the input src_node is same as the current_src_node under observation

		dest_list.append(dest_node)		#add the dest_node to the existing dest_list (contains all destinations)


	else:								#if input src_node is a new observation

		if current_src_node:			#verifying that I have a valid current_src_node and not a None value

			dest_list.sort()			#lexically sort the adj nodes

			print(current_src_node,"\t",dest_list)	#stream out , end of observation for a src_node

			#create an entry in V vector-file

			file_handler.write(current_src_node+'\t'+'1'+'\n')



		current_src_node = src_node		#get adapted to the new src_node

		dest_list = [dest_node]			#and straight-away form a list on fly


if current_src_node == src_node:

	print(current_src_node,"\t",dest_list)

	file_handler.write(current_src_node+'\t'+'1'+'\n')


file_handler.close()








