import sys
from pyspark import SparkConf, SparkContext
import numpy as np
import time

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










### STUDENT PAGE RANK CODE END   ###
last = time.time()
print("Total Program Time: " + str(last - first))

# Do not forget to stop the spark instance
sc.stop()

