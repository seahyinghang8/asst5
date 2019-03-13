import sys
from pyspark import SparkConf, SparkContext
import numpy as np
import time


### NOTE: DEBUG TOOL ###
def print_rdd(rdd):
	print("\n\nPrinting 1 RDD:{0} \n".format(rdd.take(1)[0]))


# Create spark context
conf = SparkConf()
sc = SparkContext(conf=conf)

# This loads the input file as an RDD, with each element being a string
# of the form "source destination" where source and destination
# are node id's representing the directed edge from node source
# to node destination. Note that the elements of this RDD are string
# types, hence you will need to map them to integers later.
lines = sc.textFile(sys.argv[1])

first = time.time()
### STUDENT PAGE RANK CODE START ###

# Create matrix M - broken into rows
# Convert lines to pairs
pairs = lines.map(lambda l: tuple([int(num) for num in l.split('\t')])).distinct()
reverse_pairs = pairs.map(lambda p: tuple(reversed(p)))
# Count the number of elements in the graph
num_elem = pairs.flatMap(lambda p: p).distinct().count()
# Find the number of outgoing edges from each node
outgoing_count_dict = pairs.countByKey()
# Create a row of M
# function that maps an iterator of nodes to a numpy array
def nodesToVec(nodes, deg, n):
	vec = np.zeros((1, n))
	for node in nodes:
		if (node not in deg): continue
		vec[0][node - 1] = 1. / deg[node]
	return vec

M = reverse_pairs.groupByKey().map(lambda (k, v): (k, nodesToVec(v, outgoing_count_dict, num_elem)))

#print_rdd(M)


### STUDENT PAGE RANK CODE END   ###
last = time.time()
print("Total Program Time: " + str(last - first))

# Do not forget to stop the spark instance
sc.stop()

