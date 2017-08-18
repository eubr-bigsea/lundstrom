#
# This file is just an usage example. 
# Here we define a list of queries and a list of amount of executors.
# The Spark logs should be placed at the ./data/ folder, organized by query and number of executors.
# In this example, the folders for all experiments are read and all those logs files are processed.
#
def run_model(num_nodes, num_cores, ram_size, datasize, query, confdir, baseConfig):
	from compss.parser import lundstrom_from_logdir

	# determining log dir
	logdir = confdir % (query, num_nodes, num_cores, ram_size, datasize)

	# running lundstrom
	results = lundstrom_from_logdir(num_nodes*num_cores, logdir, baseConfig)

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
