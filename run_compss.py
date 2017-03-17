#
# This file is just an usage example. 
# Here we define a list of queries and a list of amount of executors.
# The Spark logs should be placed at the ./data/ folder, organized by query and number of executors.
# In this example, the folders for all experiments are read and all those logs files are processed.
#
from compss.parser import lundstrom_from_logdir
import os, sys

# Getting params
num_nodes = int(sys.argv[1])
num_cores = int(sys.argv[2])
# ram_size = sys.argv[3]
# datasize = int(sys.argv[4])
profile = sys.argv[3]

# determining log
dir_path = os.path.dirname(os.path.realpath(__file__))
logdir = dir_path + "/data/compss/%s/%d_%d/" % (profile, num_nodes, num_cores)

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
print meanAppTime
print meanPredTime