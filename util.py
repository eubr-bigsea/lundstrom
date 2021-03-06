# 
# Subtract two timestamps returning a value in miliseconds
#
def sub_unix_timestamps(end, start):
	from datetime import datetime, timedelta
	duration = end-start
	time = str(duration)
	timec = timedelta(milliseconds=duration)
	ms = timec.total_seconds() * 1000.0
	return ms

def datetime_to_unix_timestamp(datetime_str):
	import time
	import datetime

	# format 2017-02-20 12:42:32,066
	uts = time.mktime(datetime.datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S,%f").timetuple())
	return uts

def str_to_datetime(date_str):
	from datetime import datetime
	return datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S,%f')

def sub_str_datetimes(end_date, start_date):
	start = str_to_datetime(start_date)
	end = str_to_datetime(end_date)

	delta = end - start
	return delta.total_seconds()*1000.0

def sub_datetimes(end_date, start_date):
	delta = end_date - start_date
	return delta.total_seconds()*1000.0

def read_files(logpath):
	import os
	return [s for s in os.listdir(logpath) if not s.startswith('.DS_Store') and not os.path.isdir(logpath + s)]
	#return [s for s in os.listdir(logpath) if s.startswith('app') or s.startswith('eventLogs')]

def read_file(filepath):
	return tuple(open(filepath, 'r'))

def write_file(filename, data):
	text_file = open(filename, "w")
	text_file.write(data)
	text_file.close()

def write_file_from_matrix(filename, data):
	filedata = ""
	for i in data:
		for j in i:
			filedata += "%f " % j
		filedata += "\n"
	
	write_file(filename, filedata)

def find_interjob_parents(stages):
	possibleParents = []
	for stage in stages:
		if len(stage["parents"]) == 0:
			for parentId in possibleParents:
				if parentId != stage["id"] and not parentId in stage["overlap"] and not parentId in stage["parents"]:
					stage["parents"].append(parentId)

		possibleParents.append(stage["id"])
		possibleParents.extend(stage["overlap"])

	return stages

def parse_stages_as_tree(stages):
	level = -1
	levels = {}
	visited = []
	for stage in stages:
		if not stage["id"] in visited:
			level+=1
			levels[level] = [stage["id"]]
			visited.append(stage["id"])

		for overlapId in stage["overlap"]:
			if not overlapId in visited:
				visited.append(overlapId)
				levels[level].append(overlapId)

	return levels

def hash_tree(tree):
	dhash = str(len(tree)) + ":" + "_".join([str(len(tree[i])) for i in tree])
	return dhash

def app_is_equal(stages1, stages2):
	tree1 = parse_stages_as_tree(stages1)
	hash1 = hash_tree(tree1)

	tree2 = parse_stages_as_tree(stages2)
	hash2 = hash_tree(tree2)

	return hash1 == hash2

def plot_DAG(stages):
	import networkx as nx
	import numpy as np
	import matplotlib.pyplot as plt

	# find interjob parents
	stages = find_interjob_parents(stages)

	# find edges
	edges = []
	for stage in stages:
		if len(stage["parents"]) > 0:
			for parentId in stage["parents"]:
				if not parentId in edges:
					e = (parentId, stage["id"])
					edges.append(e)

	# find the x,y position of each node in the tree
	tree = parse_stages_as_tree(stages)
	pos = {}
	labels = {}
	n_levels = len(tree)
	for level in tree:
		n_stages = len(tree[level])
		gap = 1.0/n_stages
		x = gap/2.0
		for j, stage in enumerate(tree[level]):
			xpos = j*gap+x
			ypos = n_levels-level
			pos[stage] = (xpos, ypos)
			labels[stage] = stage

	# creating DiGraph and plotting it
	G = nx.DiGraph()
	G.add_edges_from(edges)
	nx.draw_networkx_labels(G,pos,labels,font_size=10)
	nx.draw_networkx_nodes(G, pos, cmap=plt.get_cmap('jet'), label=labels, node_color='silver', node_size=800)
	nx.draw_networkx_edges(G, pos, edgelist=G.edges(), arrows=True)
	plt.show()