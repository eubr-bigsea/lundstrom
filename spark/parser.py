import json
from util import sub_unix_timestamps

# read log line by line as json
def read_log(logpath):
	return [json.loads(line.replace("\r", "").replace("\n", "")) for line in tuple(open(logpath, 'r'))]

def parse_DAG(logPath):
	events = read_log(logPath)

	stage_jobs = {}
	open_stages = []
	app = {}
	stages = []
	for log in events:

		# extract start and end times for the application
		# allows to calculate the application execution time
		if log["Event"] == "SparkListenerApplicationStart":
			appStart = log["Timestamp"]
		elif log["Event"] == "SparkListenerApplicationEnd":
			appEnd = log["Timestamp"]

		# extract a list of stages for each Job
		# this way it is possible to create the execution DAG automatically
		elif log["Event"] == "SparkListenerJobStart":
			jid = log["Job ID"]
			for sid in log["Stage IDs"]:
				stage_jobs[sid] = jid

		# compute stages to obtain overlaps
		# also needed to create the execution DAG automatically
		elif log["Event"] == "SparkListenerStageSubmitted":
			# extract the stage ID
			# towards Lundstrom's model, a stage can be abstracted as a task
			# similarly, in spark a stage is submitted to the Scheduler as a task
			# considering these facts, it is feasible to consider a spark stage as a task in Lundstrom's model
			stageId = int(log["Stage Info"]["Stage ID"])

			# determining the DAG node ID
			nid = "J%sS%s" % (stage_jobs[stageId], stageId)

			# determining the parent stages
			parents = [ "J%sS%s" % (stage_jobs[parentId], parentId) for parentId in log["Stage Info"]["Parent IDs"] ]

			# initialize
			app[nid] = {"id": nid, "job_id":stage_jobs[stageId], "stage_id":stageId, "overlap": [], "parents": parents, "start": False, "end": False}

			# add starting stage to list of overlap stages of those who are still running
			for opened_nid in open_stages:
				if opened_nid != nid:
					app[opened_nid]["overlap"].append(nid)
					app[nid]["overlap"].append(opened_nid)

			open_stages.append(nid)
		elif log["Event"] == "SparkListenerStageCompleted":
			# extract the stage ID
			stageId = int(log["Stage Info"]["Stage ID"])

			# determining the DAG node ID
			nid = "J%sS%s" % (stage_jobs[stageId], stageId)

			# catalog data
			app[nid]["start"] = log["Stage Info"]["Submission Time"]
			app[nid]["end"] = log["Stage Info"]["Completion Time"]

			stages.append(app[nid])
			# removing terminated stages from opened stages list
			open_stages.remove(nid)

	appCompletionTime = sub_unix_timestamps(appEnd, appStart)
	return appCompletionTime, stages