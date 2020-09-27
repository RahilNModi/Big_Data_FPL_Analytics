#! /usr/bin/python3

import sys

current_node = None

current_value = 0.0

node = None

for records in sys.stdin:

	record = records.strip()

	node,contributions_by_linked_nodes = record.split('\t',1)

	try:

		contributions_by_linked_nodes=float(contributions_by_linked_nodes)

	except ValueError:

		continue


	if current_node == node:

		current_value+=contributions_by_linked_nodes

	else:

		if current_node:

			page_rank_of_node = round(0.15 + 0.85*(current_value),5)

			print(current_node,",",page_rank_of_node)


		current_node = node

		current_value = contributions_by_linked_nodes

if current_node==node:

	current_value+=contributions_by_linked_nodes

	page_rank_of_node = round(0.15 + 0.85*(current_value),5)

	print(current_node,',',current_value)




