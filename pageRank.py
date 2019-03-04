import sys
from pyspark import SparkConf, SparkContext
import numpy as np
import time

# Create spark context
conf = SparkConf()
sc = SparkContext(conf=conf)

# This loads the input file as an RDD, with each element being a string
# of the form "source destination" where source and destination
# are integer node id's representing the directed edge from node source
# to node destination.
lines = sc.textFile(sys.argv[1])

### STUDENT PAGE RANK CODE ###
first = time.time()











last = time.time()
print("Total Program Time: " + str(last - first))

# Do not forget to stop the spark instance
sc.stop()

