import os, sys, json, getopt

def main(argv):
	# Getting params
	try:
	  	opts, args = getopt.getopt(argv,"bn:c:r:d:q:p:l:sn")
	except getopt.GetoptError:
	  	print 'python run.py -n <nodes> -c <cores> -r <ram> -d <data> -q <query> -p <platform> -f <conffile> -b'
		print 'or'
		print 'python run.py -l <logdir> -p <platform>'
	  	sys.exit(2)

	config_file = False
	baseConfig = False
	cores_to_predict = False
	confdir = False

	# iterating over the params
	printStages = False
	for opt, value in opts:
		if opt == "-s":
			printStages = True
		if opt == "-l":
			confdir = value
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

	if confdir == False:
		# reading config
		if not config_file:
			config_file = os.path.dirname(os.path.realpath(__file__)) + "/config.json"

		config = json.loads(open(config_file, 'r').read())

		if platform == "compss":
			# determining log dir
			confdir = str(config["COMPSS_LOG_DIR"])
		elif platform == "spark":
			# determining log dir
			confdir = str(config["SPARK_LOG_DIR"])
		else:
			print "ERROR: Only 'compss' and 'spark' are available. Please, type the platform name in lowercase."
			sys.exit()

	if platform == "compss":
		from run_compss import run_model
	elif platform == "spark":
		from run_spark import run_model

	# executing model
	result = run_model(data, cores_to_predict, confdir)

	if printStages == False:
		result.pop('stages', None)

	print result
	sys.exit()


if __name__ == "__main__":
   main(sys.argv[1:])
