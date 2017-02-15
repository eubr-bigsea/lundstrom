#
# This file is just an usage example. 
# Here we define a list of queries and a list of amount of executors.
# The Spark logs should be placed at the ./data/ folder, organized by query and number of executors.
# In this example, the folders for all experiments are read and all those logs files are processed.
#
from run_lundstrom import lundstrom_from_logdir
from util import plot_DAG

queries = ["q26", "q52"]
servers = [3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 17, 18, 19, 20, 21, 22, 23, 24]
list_pred = {}
list_app = {}
list_elapsed = {}

for q in queries:
	list_app[q] = []
	list_pred[q] = []
	list_elapsed[q] = []

	for K in servers:
		logdir = "./data/%s/%dexecutors/" % (q, K)
		results = lundstrom_from_logdir(K, logdir)
		meanAppTime = 0
		meanPredTime = 0
		meanElapsed = 0
		for appTime, predTime, elapsed, app, tree in results:
			meanAppTime+=appTime
			meanPredTime+=predTime
			meanElapsed+=elapsed
			# plot_DAG(app)
		meanAppTime /= len(results)
		meanPredTime /= len(results)
		meanElapsed /= len(results)
		list_app[q].append(meanAppTime)
		list_pred[q].append(meanPredTime)
		list_elapsed[q].append(meanElapsed)

# plot chart with a line, points and vertical errorbars
def plot(x,y,p):
	import matplotlib
	import matplotlib.pyplot as plt
	fig,ax = plt.subplots()
	fig.patch.set_facecolor('white')
	ax.plot(x, y, '-o', label=('Real'))
	ax.plot(x, p, '--', label=('Estimated'))

	max_y = max([max(y),max(p)])
	min_y = min([min(y),min(p)])

	plt.ylabel("Response time (ms)")
	plt.xlabel("Number of servers")
	plt.legend(loc='upper right', shadow=True)
	plt.xticks(x)
	plt.axis([x[0], x[len(x)-1], 0, max_y+min_y])
	plt.show()

def print_table(x,y,p,els):
	print " %7s | %7s | %7s | %10s | %11s" % ("servers", "real", "estimated", "rel. error %", "elapsed time (ms)")
	j = 0
	for i in x:
		# print header
		print "%8d | %7.0f | %9.0f | %10.2f | %11.2f" % (i, y[j], p[j], (abs(p[j]-y[j])/y[j])*100, els[j])
		j+=1
	print "----------------------------------------------"
	print ""

for q in queries:

	print "Printing query %s table" % q
	print_table(servers, list_app[q], list_pred[q], list_elapsed[q])

	print "Plotting query %s chart" % q
	plot(servers, list_app[q], list_pred[q])

