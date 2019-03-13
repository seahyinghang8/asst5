import sys
from pyspark import SparkConf, SparkContext
import numpy as np
import time
import re

# Create spark context
conf = SparkConf()
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# This loads the input file as an RDD, with each element being a string
# of the form "source destination" where source and destination
# are node id's representing the directed edge from node source
# to node destination. Note that the elements of this RDD are string
# types, hence you will need to map them to integers later.
lines = sc.textFile(sys.argv[1])

first = time.time()
### STUDENT PAGE RANK CODE START ###

# Setup program constants
num_steps = 40 #100
beta = 0.8

# Create matrix M - broken into rows
# Convert lines to pairs
pairs = lines.map(lambda l: tuple([int(num) for num in re.split('[ |\t]', l)])).distinct()
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

# Initialize pageRank vector r
r = np.ones((num_elem, 1)) / num_elem
r_prev = r.copy()

# Calculate teleport probability
tele_prob = (1. - beta) / num_elem

# Iterate through the number of steps
for _ in range(num_steps):
 	rdd_r = M.map(lambda (k, v): (k, (v.dot(r_prev) * beta)[0][0]))
	r[:] = tele_prob

	for (node, val) in rdd_r.collect():
		r[node - 1][0] += val

	# Swap the assignments
	temp = r_prev
	r_prev = r
	r = temp

# # For small-graph sanity check
# print("\n\nThe maximum node is {} with the value {}.\n\n".format(r.argmax() + 1, r[r.argmax()]))

# # Print out for large graphs
# sorted_indices = np.argsort(r.squeeze(), axis=0)
# print("The top 5 nodes are...")
# for i in range(5):
# 	print("Node {}\t Val {}".format(sorted_indices[num_elem - 1 - i] + 1, r[sorted_indices[num_elem - 1 - i]]))

# print("The bottom 5 nodes are...")
# for i in range(5):
# 	print("Node {}\t Val {}".format(sorted_indices[i] + 1, r[sorted_indices[i]]))

### STUDENT PAGE RANK CODE END   ###
last = time.time()
print("Total Program Time: " + str(last - first))

# Do not forget to stop the spark instance
sc.stop()

