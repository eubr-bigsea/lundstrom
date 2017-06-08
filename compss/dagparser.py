import json, sys, re, os, math, time
from util import write_file, sub_unix_timestamps, sub_str_datetimes, sub_datetimes, read_files, parse_stages_as_tree, hash_tree, datetime_to_unix_timestamp, write_file_from_matrix
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
	stages = []

	first_task_datetime = 0
	first_task = 0
	end_task = 0
	end_task_datetime = 0
	
	# iterating over all events to fetch starting and ending times of tasks
	for log in events:

		# finding a start task event - task line
		if "@doSubmit" in log and "Task:" in log:
			m = pd.search(log)
			datetime = m.group(0)
			# timestamp = datetime_to_unix_timestamp(datetime)

			m = pt.search(log)
			job_id = int(m.group(1))
			task_id = int(m.group(2))

			if first_task_datetime == 0:
				first_task_datetime = datetime
				first_task = task_id

		# finding an end task event
		if "@endTask" in log and "status FINISHED" in log:
			m = pd.search(log)
			datetime = m.group(0)
			# timestamp = datetime_to_unix_timestamp(datetime)

			m = pte.search(log)
			task_id = int(m.group(1))

			end_task_datetime = datetime
			end_task = task_id

		if "@waitForTask" in log:

			# stage without task
			if first_task_datetime == 0:
				first_task_datetime = end_task_datetime
				m = pd.search(log)
				end_task_datetime = m.group(0)

			stage = {"id": end_task, "job_id":end_task, "task_id":end_task, "overlap": [], "duration": sub_str_datetimes(end_task_datetime, first_task_datetime), "start_datetime": first_task_datetime, "end_datetime": end_task_datetime, "start": {"taskId": first_task, "datetime": first_task_datetime}, "end": {"taskId": end_task, "datetime": end_task_datetime}}
			stages.append(stage)
			first_task_datetime = 0

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

		ahash = len(app)
		if not dags.has_key(ahash):
			dags[ahash] = []

		dags[ahash].append((appTime, app))

	return dags

def mean_dag(dags):
	meanDags = {}
	for ahash in dags:
		i = 0
		k = len(dags[ahash])
		meanAppTime = 0

		if not meanDags.has_key(ahash):
			meanDags[ahash] = []

		for appTime, app in dags[ahash]:
			meanAppTime += appTime
			i+=1
			# print appTime
			for idx, stage in enumerate(app):

				if len(meanDags[ahash]) == idx:
					meanDags[ahash].append(0)

				meanDags[ahash][idx] += stage["duration"]

				if k == i:
					meanDags[ahash][idx] /= k

			if k == i:
				meanAppTime/=k

	return meanAppTime, meanDags			



def lundstrom_from_logdir(K, logdir):
	dags = parse_logs(logdir)
	# for ahash in dags:
	# 	i = 0
	# 	for appTime, app in dags[ahash]:
	# 		summ = 0
	# 		data=""
	# 		for idx, stage in enumerate(app):
	# 			data += "%f " % stage["duration"]
	# 		write_file("./temp/%d.txt"%i, data)
	# 		i+=1
	# 		print i

	# sys.exit()
	results = []
	
	for dag in dags:
		executions = float(len(dags[dag]))

		meanAppTime = 0.0
		meanResponse = 0.0
		meanDemand = 0.0
		meanOverlap = 0.0

		for appTime, app in dags[dag]:
			appTime, stages, response, demand, overlap = extract_data(K, appTime, app)
			meanAppTime += appTime
			meanResponse += response
			meanDemand += demand
			meanOverlap += overlap

		meanAppTime /= executions
		meanResponse /= executions
		meanDemand /= executions
		meanOverlap /= executions

		predTime, elapsed = lundstrom(len(app), K, meanResponse, meanDemand, meanOverlap)
		results.append((meanAppTime, predTime, elapsed, app, 0))

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