#
# This file is just an usage example. 
# Here we define a list of queries and a list of amount of executors.
# The Spark logs should be placed at the ./data/ folder, organized by query and number of executors.
# In this example, the folders for all experiments are read and all those logs files are processed.
#
from run_lundstrom import lundstrom_from_logdir
import os, sys, json

# Getting params
num_nodes = int(sys.argv[1])
num_cores = int(sys.argv[2])
ram_size = sys.argv[3]
datasize = int(sys.argv[4])
query = sys.argv[5]

# reading config file
config_file = sys.argv[6] if len(sys.argv) == 7 else "./config.json"
config = json.loads(open(config_file, 'r').read())

# determining log dir
# dir_path = os.path.dirname(os.path.realpath(__file__))
confdir = str(config["LOG_DIR"])
logdir = confdir % (num_nodes, num_cores, ram_size, datasize, query)

# running lundstrom
results = lundstrom_from_logdir(num_nodes*num_cores, logdir)
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
print meanPredTime