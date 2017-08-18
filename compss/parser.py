import json, sys, re, os, math, time
from util import sub_unix_timestamps, sub_str_datetimes, sub_datetimes, read_files, parse_stages_as_tree, hash_tree, datetime_to_unix_timestamp, write_file_from_matrix
from compss.lundstrom import extract_data

# read log line by line as json
def read_log(logpath):
	return [line.replace("\r", "").replace("\n", "") for line in tuple(open(logpath, 'r'))]

# parse DAG
def parse_DAG(logPath):
	events = read_log(logPath)

	# defining a pattern to match the event date
	# format: (2017-02-20 12:42:32,066)
	patternDatetime = "(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2}),(\d{3})"
	pd = re.compile(patternDatetime)

	# defining a pattern to match the job and task id
	patternTask = "New Job (\d*) \(Task: (\d*)\)"
	pt = re.compile(patternTask)

	# definind a pattern to match task id on @endTask event
	patternTaskEnd = "Notification received for task (\d*) with end status FINISHED"
	pte = re.compile(patternTaskEnd)
	
	app = {}
	open_stages = []
	stages = []
	
	# iterating over all events to fetch starting and ending times of tasks
	for log in events:

		# finding a start task event - task line
		if "@doSubmit" in log and "Task:" in log:
			m = pd.search(log)
			datetime = m.group(0)
			timestamp = datetime_to_unix_timestamp(datetime)

			m = pt.search(log)
			job_id = int(m.group(1))
			task_id = int(m.group(2))

			nid = task_id
			app[nid] = {"id": nid, "job_id":job_id, "task_id":task_id, "overlap": [], "start": timestamp, "start_datetime": datetime, "end": False, "end_datetime": False}

			# add starting stage to list of overlap stages of those who are still running
			for opened_nid in open_stages:
				if opened_nid != nid:
					app[opened_nid]["overlap"].append(nid)
					app[nid]["overlap"].append(opened_nid)

			open_stages.append(nid)


		# finding an end task event
		if "@endTask" in log and "status FINISHED" in log:
			m = pd.search(log)
			datetime = m.group(0)
			timestamp = datetime_to_unix_timestamp(datetime)

			m = pte.search(log)
			task_id = int(m.group(1))

			nid = task_id

			app[nid]["end"] = timestamp
			app[nid]["end_datetime"] = datetime

			stages.append(app[nid])

			# removing terminated stages from opened stages list
			open_stages.remove(nid)

		if "@init" in log:
			m = pd.search(log)
			start = m.group(0)

		if "@noMoreTasks" in log:
			m = pd.search(log)
			end = m.group(0)

	return sub_str_datetimes(end, start), stages

def parse_logs(logpath):
	files = read_files(logpath)

	dags = {}
	for filename in files:
		appTime, app = parse_DAG(logpath + filename)
		
		tree = parse_stages_as_tree(app)
		ahash = hash_tree(tree)
		if not dags.has_key(ahash):
			dags[ahash] = []

		dags[ahash].append((appTime, app, tree))

	return dags

def lundstrom_from_logdir(K, K_to_predict, logdir):
	dags = parse_logs(logdir)
	results = []

	for dag in dags:
		executions = float(len(dags[dag]))

		meanAppTime = 0.0
		meanResponse = 0.0
		meanDemand = 0.0
		meanOverlap = 0.0

		for appTime, app, tree in dags[dag]:
			appTime, stages, response, demand, overlap = extract_data(K, K_to_predict, appTime, app)
			meanAppTime += appTime
			meanResponse += response
			meanDemand += demand
			meanOverlap += overlap

		meanAppTime /= executions
		meanResponse /= executions
		meanDemand /= executions
		meanOverlap /= executions

		predTime, elapsed = lundstrom(len(app), K, meanResponse, meanDemand, meanOverlap)
		results.append((meanAppTime, predTime, elapsed, app, tree))

	return results

def lundstrom(N, C, response, demand, overlap):
	dir_path = os.path.dirname(os.path.realpath(__file__))
	os.popen("gcc -o %s/../bin/makva %s/../bin/makva.c" % (dir_path, dir_path))
	write_files((response, demand, overlap))
	cmd = "%s/../bin/makva -N %s -C %s -e 50 -r %s/temp/response.txt -s %s/temp/demand.txt -o %s/temp/overlap.txt" % (dir_path, N, C, dir_path, dir_path, dir_path)
	start_time = time.time()
	output = os.popen(cmd).read()
	elapsed_time = time.time() - start_time
	return float(output.replace("\n","").replace("R: ", "").strip())/C, elapsed_time*1000

def write_files(data):
	response, demand, overlap = data
	dir_path = os.path.dirname(os.path.realpath(__file__))
	write_file_from_matrix(dir_path+"/temp/response.txt", response)
	write_file_from_matrix(dir_path+"/temp/demand.txt", demand)
	write_file_from_matrix(dir_path+"/temp/overlap.txt", overlap)