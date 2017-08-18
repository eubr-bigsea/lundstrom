import json, sys
from util import sub_unix_timestamps, sub_str_datetimes, sub_datetimes, str_to_datetime

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

	# diff1 = sub_datetimes(stage2["end_datetime"], stage1["start_datetime"])
	# Pr_Ej_less_Si = int(diff1 < 0)

	# diff2 = sub_datetimes(stage1["end_datetime"], stage2["start_datetime"])
	# Pr_Ei_less_Sj = int(diff2 < 0)

	# diff3 = sub_datetimes(stage2["start_datetime"], stage1["start_datetime"])
	# start_ovl = stage2["start_datetime"] if diff3 > 0 else stage1["start_datetime"]

	# diff4 = sub_datetimes(stage2["end_datetime"], stage1["end_datetime"])
	# end_ovl = stage2["end_datetime"] if diff4 < 0 else stage1["end_datetime"]

	# # overlap duration at all service centers
	# dij = K*float(sub_datetimes(end_ovl, start_ovl))

	# # residence time of task i at all service centers
	# Ri = K*float(sub_datetimes(stage1["end_datetime"], stage1["start_datetime"]))

	# # overlap probability
	# pij = 1.0 - Pr_Ej_less_Si - Pr_Ei_less_Sj

	# return (pij*dij)/Ri

	stg1_start = str_to_datetime(stage1["start_datetime"])
	stg1_end = str_to_datetime(stage1["end_datetime"])

	stg2_start = str_to_datetime(stage2["start_datetime"])
	stg2_end = str_to_datetime(stage2["end_datetime"])

	start_ovl = stg2_start if stg2_start > stg1_start else stg1_start
	end_ovl = stg2_end if stg2_end < stg1_end else stg1_end

	# overlap duration at all service centers
	dij = float(sub_datetimes(end_ovl, start_ovl))

	# residence time of task i at all service centers
	Ri = float(sub_datetimes(stg1_end, stg1_start))

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
def extract_response(K, stages):
	response = []
	for stage in stages:
		time_1_server = sub_str_datetimes(stage["end_datetime"], stage["start_datetime"])
		response.append(K*[time_1_server/K])

 	return response

#
# K - number of servers
# stages - stages object
#
def extract_demand(K, stages):
	demand = []
	for stage in stages:
		time_1_server = sub_str_datetimes(stage["end_datetime"], stage["start_datetime"])
		demand.append(K*[time_1_server/K])

 	return demand

#
# Extracted the data needed from spark log file
#
def extract_data(K, appTime, stages, baseConfig):
	import numpy as np
	response = extract_response(K, stages)
	demand = extract_demand(K, stages)
	overlap = extract_overlap(K, stages)
	return (appTime, stages, np.array(response), np.array(demand), np.array(overlap))