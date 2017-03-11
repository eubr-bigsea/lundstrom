import json
from util import sub_unix_timestamps

# read log line by line as json
def read_log(logpath):
	return [json.loads(line.replace("\r", "").replace("\n", "")) for line in tuple(open(logpath, 'r'))]

def parse_DAG(logPath):
	events = read_log(logPath)

	stage_jobs = {}
	open_stages = []
	app = {}
	stages = []
	# for log in events: