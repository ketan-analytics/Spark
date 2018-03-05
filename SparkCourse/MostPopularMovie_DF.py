from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions


def loadMovieNames():
    movieNames = {}
    with open("C:/Ketan - Personal/Self Learning/Big Data/Spark/SparkCourse/ml-100k/u.ITEM") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames


spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///c:/temp").config("spark.executor.memory","1g").appName(
    "PopularMovies_DF").getOrCreate()
nameDict = loadMovieNames()

lines = spark.sparkContext.textFile("C:/Ketan - Personal/Self Learning/Big Data/Spark/SparkCourse/ml-100k/u.data")
movies = lines.map(lambda x: Row(movieID=int(x.split()[1])))
movieDataSet = spark.createDataFrame(movies)

topMovieIds = movieDataSet.groupBy("movieID").count().orderBy("count", ascending=False).cache()
topMovieIds.show()
#
# top10 = topMovieIds.take(10)
#
# print("\n")
# for result in top10:
#     print(f'{nameDict[result[0]]},{result[1]}')
# spark.stop()
