import json, sys
from util import sub_unix_timestamps

# 
# Extract stages data including those overlaping stages (Stage id) from the application
#
def extract_stages(logpath):
	lines = tuple(open(logpath, 'r'))

	app = {}
	open_stages = []
	for line in lines:
		line = line.replace("\r", "").replace("\n", "")
		log = json.loads(line)

		if log["Event"] == "SparkListenerStageSubmitted":

			# extract the stage ID
			# towards Lundstrom's model, a stage can be abstracted as a task
			# similarly, in spark a stage is submitted to the Scheduler as a task
			# considering these facts, it is feasible to consider a spark stage as a task in Lundstrom's model
			stageId = int(log["Stage Info"]["Stage ID"])

			# The same application can have different stages
			# the Stage Name identifies the type of stage and the source code line where it is called
			stageName = log["Stage Info"]["Stage Name"]

			# initialize
			app[stageId] = {"overlap": [], "name": stageName}

			# add starting stage to list of overlap stages of those who are still running
			for openedStageId in open_stages:
				if openedStageId != stageId:
					app[openedStageId]["overlap"].append(stageId)
					app[stageId]["overlap"].append(openedStageId)

			open_stages.append(stageId)
		# Check stage completion logs
		elif log["Event"] == "SparkListenerStageCompleted":

			# extract the stage ID
			stageId = int(log["Stage Info"]["Stage ID"])

			# catalog data
			app[stageId]["start"] = log["Stage Info"]["Submission Time"]
			app[stageId]["end"] = log["Stage Info"]["Completion Time"]

			# removing terminated stages
			open_stages.remove(stageId)

	appCompletionTime = 0
	for stageId in app:
		appCompletionTime += sub_unix_timestamps(app[stageId]["end"], app[stageId]["start"])

	return appCompletionTime, app

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

	Pr_Ej_less_Si = int(stage2["end"] < stage1["start"])
	Pr_Ei_less_Sj = int(stage1["end"] < stage2["start"])

	pij = 1.0 - Pr_Ej_less_Si - Pr_Ei_less_Sj

	start_ovl = stage2["start"] if stage2["start"] > stage1["start"] else stage1["start"]
	end_ovl = stage2["end"] if stage2["end"] < stage1["end"] else stage1["end"]

	# overlap duration at all service centers
	dij = K*float(sub_unix_timestamps(end_ovl, start_ovl))
	# residence time of task i at all service centers
	Ri = K*float(sub_unix_timestamps(stage1["end"], stage1["start"]))

	# Rj = K*float(sub_unix_timestamps(stage2["end"], stage2["start"]))
	# dij = 1.0 / ( (1.0/Ri) + (1.0/Rj) )

	return (pij*dij)/Ri

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
				overlap_map[i].append(1)
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
		time_1_server = sub_unix_timestamps(stage["end"], stage["start"])
		response.append(K*[time_1_server])

 	return response

#
# K - number of servers
# stages - stages object
#
def extract_demand(K, stages):
	demand = []
	for stage in stages:
		time_1_server = sub_unix_timestamps(stage["end"], stage["start"])
		demand.append(K*[time_1_server])

 	return demand

#
# Extracted the data needed from spark log file
#
def extract_data(K, appTime, stages):
	import numpy as np
	response = extract_response(K, stages)
	demand = extract_demand(K, stages)
	overlap = extract_overlap(K, stages)
	return (appTime, stages, np.array(response), np.array(demand), np.array(overlap))