from spark.lundstrom import extract_data
from spark.parser import parse_DAG
from util import write_file_from_matrix, plot_DAG, hash_tree, parse_stages_as_tree, read_files
import sys,os, math, time

def write_files(data):
	response, demand, overlap = data
	dir_path = os.path.dirname(os.path.realpath(__file__))
	write_file_from_matrix(dir_path+"/temp/response.txt", response)
	write_file_from_matrix(dir_path+"/temp/demand.txt", demand)
	write_file_from_matrix(dir_path+"/temp/overlap.txt", overlap)

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

def lundstrom(N, C, response, demand, overlap):
	dir_path = os.path.dirname(os.path.realpath(__file__))
	os.popen("gcc -o %s/bin/makva %s/bin/makva.c" % (dir_path, dir_path))
	write_files((response, demand, overlap))
	dir_path = os.path.dirname(os.path.realpath(__file__))
	cmd = "%s/bin/makva -N %s -C %s -e 50 -r %s/temp/response.txt -s %s/temp/demand.txt -o %s/temp/overlap.txt" % (dir_path, N, C, dir_path, dir_path, dir_path)
	start_time = time.time()
	output = os.popen(cmd).read()
	elapsed_time = time.time() - start_time
	return float(output.replace("\n","").replace("R: ", "").strip())/C, elapsed_time*1000

def lundstrom_from_logdir(K, logdir):
	dags = parse_logs(logdir)
	results = []
	
	for dag in dags:	
		executions = len(dags[dag])
		meanAppTime = 0
		meanResponse = 0
		meanDemand = 0
		meanOverlap = 0

		for appTime, app, tree in dags[dag]:
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
		results.append((meanAppTime, predTime, elapsed, app, tree))

	return results