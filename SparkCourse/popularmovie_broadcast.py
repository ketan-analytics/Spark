from pyspark import SparkConf, SparkContext

def loadMovieNames():
    movieNames = {}
    with open("C:/Ketan - Personal/Self Learning/Big Data/Spark/SparkCourse/ml-100k/u.ITEM") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

conf = SparkConf().setMaster("local").setAppName("PopularMoviesBroadcast")
sc = SparkContext(conf = conf)

nameDict = sc.broadcast(loadMovieNames())

lines = sc.textFile("file:///C:/Ketan - Personal/Self Learning/Big Data/Spark/SparkCourse/ml-100k/u.data")
movies = lines.map(lambda x: (int(x.split()[1]), 1))
movieCounts = movies.reduceByKey(lambda x, y: x + y)
flipped = movieCounts.map( lambda  )
sortedMovies = flipped.sortByKey()

sortedMoviesWithNames = sortedMovies.map(lambda x: (nameDict.value[x[1]], x[0]))

results = sortedMoviesWithNames.collect()

for result in results:
    print(result)
