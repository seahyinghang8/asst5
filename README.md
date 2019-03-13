__Due Thursday March 14th, 11:59pm__

# Page Rank in Spark

## Summary
Spark is a general-purpose distributed data processing engine that is suitable for use in a wide range of circumstances. In this assignment you will learn to use the PySpark Python API for Spark, and will be implementing a Page Rank algorithm leveraging Spark.

A quick overview on Spark:
1. A Spark application runs as independent processes, coordinated by the SparkSession object in the driver program.
2. The resource or cluster manager assigns tasks to workers, one task per partition.
3. A task applies its unit of work to the dataset in its partition and outputs a new partition dataset. Because iterative algorithms apply operations repeatedly to data, they benefit from caching datasets across iterations.
4. Results are sent back to the driver application or can be saved to disk.

## First things first: VM Setup

You can follow the same instructions as in programming assignment 3's VM setup (https://github.com/stanford-cs149/asst3/blob/master/cloud_readme.md) to create another instance with the same configuration (or continue using the one you already created).

You will need to install Python and Java before installing Spark. Here are the commands for doing so:

```
sudo apt-get install python
sudo apt-get install python-pip && sudo pip install numpy
sudo apt install openjdk-8-jre-headless
```

## Before starting the page rank assignment: Spark Tutorial
Here you will learn how to write, compile, debug and execute a simple Spark program. First part of the assignment serves as a tutorial and the second part asks you to write your own Spark program. We have included a tutorial for using Python.

Section 1 explains how to download and install a stand-alone Spark instance. All operations done in this Spark instance will be performed against the files in your local file system.

Section 2 explains how to launch the Spark shell for interactively building Spark applications.

Section 3 explains how to use Spark to launch Spark applications written in an IDE or editor.

Section 4 gives an example of writing a simple word count application for Spark.

### 1) Setting up a stand-alone Spark Instance
Download and install Spark 2.2.1 on your machine (you can use wget): https://www.apache.org/dyn/closer.lua/spark/spark-2.2.1/spark-2.2.1-bin-hadoop2.7.tgz

Unpack the compressed TAR ball.

To make Spark work add the following to you ~/.profile file:\
JAVA_HOME="/usr/bin/java"\
SPARK_HOME="$HOME/spark-2.2.1-bin-hadoop2.7"\
SPARK_LOCAL_IP="127.0.0.1"\
PATH="$HOME/bin:$HOME/.local/bin:$SPARK_HOME/bin:$PATH"

### 2) Running the Spark shell
The easiest way to run your Spark applications is using the Spark shell, a REPL that let's you interactively compose your application. To start the Spark shell, do the following:

For Python:
1. Change into the directory where you unpacked the Spark binary
2. For Python use: bin/pyspark

As the Spark shell starts, you may see large amounts of logging information displayed on the screen, possibly including several warnings. You can ignore that output for now. The Spark shell is a full interpreter and can be used to write and execute Python code. For example for Python:

```
>>> print("Hello!")
Hello!
```

To learn about writing Spark applications, please read through the Spark programming guide: https://spark.apache.org/docs/2.2.0/rdd-programming-guide.html

### 3) Launching Spark Applications
The Spark shell is great for exploring a data set or experimenting with the API, but it's often best to write your Spark applications outside of the Spark interpreter using an IDE or other smart editor. Spark accepts applications written in four languages: Scala, Java, python, and R. We highly recommend python as both the language itself and the python Spark API are straightforward.

For Python, assume you have the following program in a text file called myapp.py:

```
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf()
sc = SparkContext(conf=conf)
print "%d lines" % sc.textFile(sys.argv[1]).count()
```

This short application opens the file path given as the first argument from the local working directory and prints the number of lines in it. To run this application, do the following:
1. Change into the directory where you unpacked the Spark binary
2. Run:
```
bin/spark-submit path/to/myapp.py path/to/file
```

As Spark starts, you may see large amounts of logging information displayed on the screen, possibly including several warnings. You can ignore that output for now. Regardless, near the bottom of the output you will see the output from the application. Executing the application this way causes it to be run single-threaded. To run the application with 4 threads, launch it as:
```
bin/spark-submit --master ’local[4]’ path/to/myapp.py path/to/file
```

You can replace the “4” with any number. To use as many threads as are available on your system, launch the application as:
```
bin/spark-submit --master ’local[*]’ path/to/myapp.py path/to/file
```

### 4) WordCount in Spark

The typical “Hello, world!” type of app for Spark applications is word count. The map/reduce model is particularly well suited to applications like counting words in a document. In this section, you will see how to develop a word count application using Spark in Python. Prior to reading this section, you should read through the Spark programming guide if you haven’t already.

All operations in Spark operate on data structures called RDDs, Resilient Distributed Datasets. An RDD is nothing more than a collection of objects. If you read a file into an RDD, each line will become an object (a string, actually) in the collection that is the RDD. If you ask Spark to count the number of elements in the RDD, it will tell you how many lines are in the file. If an RDD contains only two-element tuples, the RDD is known as a “pair RDD” and offers some additional functionality. The first element of each tuple is treated as a key, and the second element as a value. Note that all RDDs are immutable, and any operations that would mutate an RDD will instead create a new RDD.

For the example you can use the pg100.txt file to perform word count. It can be found the handout repo.

For this example, you will create your application in an editor instead of using the Spark shell. The first step of every such Spark application is to create a Spark context:

```
import re
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf()
sc = SparkContext(conf=conf)
```

Next, you’ll need to read the target file into an RDD:
```
lines = sc.textFile(sys.argv[1])
```

You now have an RDD filled with strings, one per line of the file. Next you’ll want to split the lines into individual words:
```
words = lines.flatMap(lambda l: re.split(r’[^\w]+’, l))
```

The flatMap() operation first converts each line into an array of words, and then makes each of the words an element in the new RDD. If you asked Spark to count the number of elements in the words RDD, it would tell you the number of words in the file. Next, you’ll want to replace each word with a tuple of that word and the number 1. The reason will become clear shortly.
```
pairs = words.map(lambda w: (w, 1))
```

The map() operation replaces each word with a tuple of that word and the number 1. The pairs RDD is a pair RDD where the word is the key, and all of the values are the number 1. Now, to get a count of the number of instances of each word, you need only group the elements of the RDD by key (word) and add up their values:
```
counts = pairs.reduceByKey(lambda n1, n2: n1 + n2)
```

The reduceByKey() operation keeps adding elements’ values together until there are no more to add for each key (word). Finally, you can store the results in a file and stop the context:
```
counts.saveAsTextFile(sys.argv[2])
sc.stop()
```

The completed file looks like:
```
import re
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf()
sc = SparkContext(conf=conf)
lines = sc.textFile(sys.argv[1])
words = lines.flatMap(lambda l: re.split(r’[^\w]+’, l))
pairs = words.map(lambda w: (w, 1))
counts = pairs.reduceByKey(lambda n1, n2: n1 + n2)
counts.saveAsTextFile(sys.argv[2])
sc.stop()
```

## Assignment: Page Rank in Spark (100 Points) due on 03/14

### Overview
You have already implemented the Page Rank algorithm in openMP. Now you will implement a similar version of the algorithm using Spark!

In this problem, you will be experimenting with a small randomly generated graph (assume graph has no dead-ends) provided at graph-full.txt. You can use graph-small.txt as a sanity check that your code is working, and for debugging. These graphs have directed edges, and the files you will be parsing consist of lines, where each line represents an edge by two nodes seperated by a space. The first node is the source node, and the second is the destination node for the edge. For example here is what a line may look like: "0 1", where 0 is the source node, and 1 is the destination node (directed edge).

There are 100 nodes (n = 100) in the small graph and 1000 nodes (n = 1000) in the full graph. You can use the graph-small.txt file for debugging and sanity check, and the graph-full.txt to get a sense of your performance. We will be additionally testing you on much larger graphs that we will release in a day or two.

You may choose to store the PageRank vector r either in memory or as an RDD, however you will not be able to store the matrix of links (M below) in memory, especially on the larger graphs we will be testing you on (that will have around 500 million nodes). Due to this, the main challenge will be figuring out how to store the Matrix of links M as an RDD, and perform the matrix-vector multiply using Spark primitives.

Note that the graph files may have duplicate edges, so you will need to take care of this case. Treat all duplicate edges as one edge (you can use Spark's .distinct() function to do this).

### Algorithm
Now let us setup the algorithm you will be implementing. Let the matrix M be an (n x n) matrix such that for any i and j between [1, n], M_{ji} = 1/deg(i) if there exists a directed edge from i to j, and 0 otherwise (Here M_{ji} is the j'th row and i'th column entry of M). Here, deg(i) is the number of outgoing edges from node i in the graph. If there are multiple edges in the same direction between two nodes, treat them as a single edge.

By the definition of PageRank, assuming 1 − β to be the teleport probability, and denoting the PageRank vector by the column vector r, we have the following equation:
```
r = 1[(1 - β)/n] + β*M*r,
```

where 1[...] is the (n × 1) vector with all entries equal to (1 - β)/n, and M*r computes the matrix-vector multiplication between the matrix of links M, and the page rank vector r.

Based on this equation, the iterative procedure to compute PageRank works as follows:
```
1. Initialize r = 1[1/n]
2. For i from 1 to k, iterate: r = 1[(1 - β)/n] + β*M*r
```

You will need to figure out the best way to store the matrix M as an RDD (to simplify the matrix-vector multiplication in each iteration). You CAN NOT and SHOULD NOT store the matrix M in memory (keep it in RDD form in Spark). It will not fit in memory on the larger graphs. You must perform the matrix-vector multiplication above using Spark functions. You will also be tested on performance through timing we have added into the starter code. We provide reference times for your benefit. Write your code in between where the STUDENT CODE comments indicate in order to properly time your solution, otherwise you will not receive credit for the performance portion.

We recommend that you use numpy in other parts of your code to perform vector additions, dot products, etc... Here is a few numpy functions you may find useful:
1. https://docs.scipy.org/doc/numpy/reference/generated/numpy.dot.html
2. https://docs.scipy.org/doc/numpy/reference/generated/numpy.array.html
3. https://docs.scipy.org/doc/numpy-1.13.0/reference/generated/numpy.sum.html

Here is a general tutorial for those new to numpy: http://cs231n.github.io/python-numpy-tutorial/#numpy-math

### Handin Requirements
Run the aforementioned iterative process in Spark for 100 iterations (assuming β = 0.8) and obtain the PageRank vector r. The matrix M can be large and should be processed as an RDD in your solution. Compute the following:

1. List the top 5 node ids with the highest PageRank scores.
2. List the bottom 5 node ids with the lowest PageRank scores.

For a sanity check, we have provided a smaller dataset (graph-small.txt). In that dataset, the top node has id 53 with value 0.036 after 40 iterations (you can use this value to help debug). We will be grading you on your results for graph-full.txt and graph-large.txt. We give you a file pageRank.py to write your code in, with basic starter code that starts your Spark context and reads in the input text file as an RDD. You will also be reporting the total time it took your program to run. The starter code already wraps the code you will write in timing that is printed out at the very end (report this number in seconds). Our reference solution takes less than ~12-13 seconds for 100 iterations on graph-full.txt (on 32 vCPUs) and less than 20 seconds on graph-large.txt..

We expect you to use Spark for all operations on the data (including performing the matrix-vector multiply). You can use numpy or regular python for computing dot products and other arithmetic, but any other data computation should leverage Spark.

What you need to turn in (in a zipped file to Canvas):
1. Turn in your code in pageRank.py
2. Turn in a .txt file with the top 5 node ids and their PageRank scores, and the bottom 5 node ids and their PageRank scores for graph-full.txt and graph-large.txt after 100 iterations. This is worth 70 points for correctness.
3. In the same text file, include the total time it took your code to run on graph-full.txt and graph-large.txt. This is worth 30 points for performance.










