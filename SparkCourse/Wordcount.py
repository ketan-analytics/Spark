# Program 1 

import re
from pyspark import SparkConf, SparkContext

def normalizewords(text):
     return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("file:///C:/Ketan - Personal/Self Learning/Big Data/Spark/SparkCourse/book.txt")
words = input.flatMap(normalizewords)
#wordCounts = words.countByValue()

wordCounts=words.map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y)
wordCountsSorted=wordCounts.map(lambda x: (x[1], x[0])).sortByKey()

results = wordCountsSorted.collect()

for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if (word):
        print(word.decode() + ":\t\t" + count)



#
# def normalizewords(line):
#     return
#     return re.compile(r'\W+', re.UNICODE).split(text.lower())
#
#
# conf = SparkConf().setMaster("local").setAppName("WordCount")
# sc = SparkContext(conf=conf)
#
# input = sc.textFile("file:///C:/Ketan - Personal/Self Learning/Big Data/Spark/SparkCourse/book.txt")
# words = input.flatMap(normalizewords)
#
# wordCountsMap = words.map(lambda x: (x, 1)).filter(lambda (x, y): x not in bad_word_list)
#
# wordCountsMapReduced = wordCountsMap.reduceByKey(lambda x, y: x + y)
# wordCountsSorted = wordCountsMapReduced.map(lambda (x, y): (y, x)).sortByKey()
#
# results = wordCountsSorted.collect()
#
# for result in results:
#     print(result)
