import os, sys, json

# Getting params
num_nodes = int(sys.argv[1])
num_cores = int(sys.argv[2])
ram_size = sys.argv[3]
datasize = sys.argv[4]
query = sys.argv[5]
platform = sys.argv[6]

# reading config file
config_file = sys.argv[7] if len(sys.argv) == 8 else "./config.json"
config = json.loads(open(config_file, 'r').read())

if platform == "compss":
	from run_compss import run_model
	# determining log dir
	confdir = str(config["COMPSS_LOG_DIR"])
elif platform == "spark":
	from run_spark import run_model
	# determining log dir
	confdir = str(config["SPARK_LOG_DIR"])
else:
	print "ERROR: Only 'compss' and 'spark' are available. Please, type the platform name in lowercase."
	sys.exit()

result = run_model(num_nodes, num_cores, ram_size, datasize, query, confdir)
print result
sys.exit()