import json, sys, math
from util import sub_unix_timestamps
from extrapolation import lazyFifoScheduling, averageCalc, factorCalc

#
# Calculate overlap factor
# The overlap factor, is defined as the fraction of task i residence time
# for which task i overlaps with task j
# (MAKVA & LUNDSTROM, p. 261)
#
# stage1 - task i
# stage2 - task j
#
def overlap_factor(K, stage1, stage2):
	# MAK & LUNDSTROM P. 263
	# theta_i_j = pij * dij / Ri
	#
	# theta = overlap factor
	# pij = probability of i starting before j
	# dij = overlap duration time
	# Ri = task i response time

	# Pr_Ej_less_Si = int(stage2["end"] < stage1["start"])
	# Pr_Ei_less_Sj = int(stage1["end"] < stage2["start"])

	# pij = 1.0 - Pr_Ej_less_Si - Pr_Ei_less_Sj
	if stage1["end"] < stage2["start"] or stage2["end"] < stage1["start"]:
		return 0

	start_ovl = stage2["start"] if stage2["start"] > stage1["start"] else stage1["start"]
	end_ovl = stage2["end"] if stage2["end"] < stage1["end"] else stage1["end"]

	# overlap duration at all service centers
	dij = float(sub_unix_timestamps(end_ovl, start_ovl))

	# residence time of task i at all service centers
	Ri = float(sub_unix_timestamps(stage1["end"], stage1["start"]))

	# Rj = K*float(sub_unix_timestamps(stage2["end"], stage2["start"]))
	# dij = 1.0 / ( (1.0/Ri) + (1.0/Rj) )

	return dij/Ri

#
# stages - stages object
#
def extract_overlap(K, stages):
	# create the overlap matrix
	overlap_map = []
	for i, stage_i in enumerate(stages):
		overlap_map.append([])
		for j, stage_j in enumerate(stages):
			if i == j:
				# the theta for i==j is actually 1, however the arrival time equation sum considers i=/=j only.
				# This condition is not implemented on the makva.c file, so we should use theta=0.
				overlap_map[i].append(0)
			else:
				overlap_map[i].append(0)

	# populate the matrix with overlap factors
	for i, stage_i in enumerate(stages):
		for j, stage_j in enumerate(stages):
			if stage_j["id"] in stage_i["overlap"]:

				# calc overlap factor
				# the overlap factor, is defined as the fraction of task i residence time
				# for which task i overlaps with taskj
				# (MAKVA & LUNDSTROM, p. 261)
				factor = overlap_factor(K, stage_i, stage_j)
				overlap_map[i][j] = factor

	return overlap_map

#
# K - number of servers
# stages - stages object
#
def extract_response(K, stages, K_to_predict):
	response = []
	for stage in stages:
		time_1_server = sub_unix_timestamps(stage["end"], stage["start"])
		response.append(K_to_predict*[time_1_server])
 	return response

#
# K - number of servers
# stages - stages object
#
def extract_demand(K, stages, K_to_predict):
	demand = []
	for stage in stages:
		time_1_server = sub_unix_timestamps(stage["end"], stage["start"])
		demand.append(K_to_predict*[time_1_server])
 	return demand

#
# Extracted the data needed from spark log file
#
def extract_data(K, K_to_predict, appTime, stages):
	import numpy as np

	# base config
	# factor = float(K)/float(K_to_predict)

	response = extract_response(K, stages, K_to_predict)
	demand = extract_demand(K, stages, K_to_predict)
	overlap = extract_overlap(K, stages)
	return (appTime, stages, np.array(response), np.array(demand), np.array(overlap))
