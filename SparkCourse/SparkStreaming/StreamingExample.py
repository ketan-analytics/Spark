from pyspark.sql import SparkSession

spark=SparkSession.builder.master("local[2]").appName("SparkStreaming_ActivityData").getOrCreate()

static=spark.read.json("file:///C:/Ketan - Personal/Self Learning/Big "
                       "Data/Spark/Spark-The-Definitive-Guide-master/data/activity-data")
print(dataSchema=static.schema)