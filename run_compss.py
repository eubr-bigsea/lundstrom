#
# This file is just an usage example. 
# Here we define a list of queries and a list of amount of executors.
# The Spark logs should be placed at the ./data/ folder, organized by query and number of executors.
# In this example, the folders for all experiments are read and all those logs files are processed.
#
from compss.parser import lundstrom_from_logdir
import os, sys, json

# Getting params
num_cores = int(sys.argv[1])
fragments = int(sys.argv[2])
points = sys.argv[3]
query = sys.argv[4]

# reading config file
config_file = sys.argv[5] if len(sys.argv) == 6 else "./config.json"
config = json.loads(open(config_file, 'r').read())


# determining log dir
# dir_path = os.path.dirname(os.path.realpath(__file__))
confdir = str(config["COMPSS_LOG_DIR"])
logdir = confdir % (query, num_cores, fragments, points)

# running lundstrom
results = lundstrom_from_logdir(num_cores, logdir)
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

print '{"meanAppTime": %f, "meanPredTime": %f, meanElapsedTime: %f}' % (meanAppTime, meanPredTime, meanElapsed)