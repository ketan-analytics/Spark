import sys
from pyspark import SparkConf, SparkContext
from pyspark.mllib.recommendation import ALS, Rating


def loadMovieNames():
    movieNames = {}
    with open("C:/Ketan - Personal/Self Learning/Big Data/Spark/SparkCourse/ml-100k/u.ITEM") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1].encode('ascii', 'ignore')
    return movieNames


conf = SparkConf().setMaster("local[2]").setAppName("MovieRecommendation_Mlib_ALS")
sc = SparkContext(conf=conf)

nameDict = loadMovieNames()

data = sc.textFile("file:///C:/Ketan - Personal/Self Learning/Big Data/Spark/SparkCourse/ml-100k/u.data")
ratings = data.map(lambda l: l.split()).map(lambda l: Rating(int(l[0]), int(l[1]), float(l[2])))

# Training Recommendation Model
print("Training Recommendation Model")
rank = 10
numIterations = 20
model = ALS.train(ratings, rank, numIterations)

userID = int(sys.argv[1])

print(f'Ratings for User ID:{userID}')
userRatings = ratings.filter(lambda l: l[0] == userID)
for rating in userRatings.collect():
    print(f'{nameDict[int(rating[1])]}:{str(rating[2])}')

print(f'Top 10 Recommendation for User ID:{userID}')
recommendations = model.recommendProducts(userID, 10)
for recommendation in recommendations:
    print(f'Movie: {nameDict[int(recommendation[1])]}: Score {str(recommendation[2])}')
