def averageCalc(stage, K, K_to_predict):
	avg_task_time = stage["task_time_sum"]/stage["total_tasks"]
	turns = math.ceil(float(stage["total_tasks"])/float(K_to_predict))
	time_1_server = turns*avg_task_time
	return time_1_server

def factorCalc(stage, K, K_to_predict):
	factor = float(K)/float(K_to_predict)
	time_1_server = sub_unix_timestamps(stage["end"], stage["start"])*factor
	return time_1_server

def lazyFifoScheduling(stage, K, K_to_predict):
	time_1_server = max_server_lazy(stage, K_to_predict)
	return time_1_server

def max_server_lazy(stage, C):
	servers = []
	for i in xrange(0, C):
		servers.append(0)

	for idx, tid in enumerate(stage["tasks"]):
		servers[idx%C] += stage["tasks"][tid]["time_spent"]

	time_1_server = max(servers)
	return time_1_server
