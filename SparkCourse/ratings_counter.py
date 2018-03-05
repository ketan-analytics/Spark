from pyspark import SparkConf, SparkContext

import collections

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

lines = sc.textFile("file:///C:/Ketan - Personal/Self Learning/Big Data/Spark/SparkCourse/ml-100k/u.data")
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
  #  print "%s %i" % (key, value)

    print(f'{key} {value}')

