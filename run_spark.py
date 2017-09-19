#
# This file is just an usage example. 
# Here we define a list of queries and a list of amount of executors.
# The Spark logs should be placed at the ./data/ folder, organized by query and number of executors.
# In this example, the folders for all experiments are read and all those logs files are processed.
#
def run_model(config_with_log, cores_to_predict, confdir):
	from run_lundstrom import lundstrom_from_logdir
	import string

	query = config_with_log["query"]
	nodes = int(config_with_log["nodes"])
	cores = int(config_with_log["cores"])
	ram = config_with_log["ram"]
	data = config_with_log["data"]

	# determining log dir
	confdir = confdir.replace("%QUERY", query)
	confdir = confdir.replace("%NODES", str(nodes))
	confdir = confdir.replace("%CORES", str(cores))
	confdir = confdir.replace("%RAM", ram)
	logdir = confdir.replace("%DATA", data)

	k = nodes*cores
	k_to_predict = int(cores_to_predict) if cores_to_predict else k

	# running lundstrom
	results = lundstrom_from_logdir(k, k_to_predict, logdir)

	meanAppTime = 0
	meanPredTime = 0
	meanElapsed = 0
	for appTime, predTime, elapsed, app, tree in results:
		meanAppTime+=appTime
		meanPredTime+=predTime
		meanElapsed+=elapsed

	meanAppTime /= len(results)
	meanPredTime /= len(results)
	meanElapsed /= len(results)

	return {"real": meanAppTime, "predicted": meanPredTime, "elapsed": meanElapsed}
