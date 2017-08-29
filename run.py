import os, sys, json, getopt

def main(argv):
	# Getting params
	try:
	  	opts, args = getopt.getopt(argv,"bn:c:r:d:q:p:")
	except getopt.GetoptError:
	  	print 'python run.py -n <nodes> -c <cores> -r <ram> -d <data> -q <query> -p <platform> -f <conffile> -b'
	  	sys.exit(2)

	config_file = False
	baseConfig = False
	cores_to_predict = False

	# iterating over the params
	for opt, value in opts:
		if opt == "-n":
			num_nodes = value
		if opt == "-c":
			num_cores = value
		if opt == "-r":
			ram_size = value
		if opt == "-d":
			datasize = value
		if opt == "-q":
			query = value
		if opt == "-p":
			platform = value
		if opt == "-f":
			config_file = value
		if opt == "-pc":
			cores_to_predict = value

	# input parameters - to predict
	data = {"nodes": num_nodes, "cores": num_cores, "ram": ram_size, "data": datasize, "query": query}

	# reading config
	if not config_file:
		config_file = os.path.dirname(os.path.realpath(__file__)) + "/config.json"

	config = json.loads(open(config_file, 'r').read())

	# executing models
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

	result = run_model(data, cores_to_predict, confdir)
	print result
	sys.exit()


if __name__ == "__main__":
   main(sys.argv[1:])