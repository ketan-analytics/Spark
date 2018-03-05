from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField,StructType,StringType,IntegerType,TimestampType
from tabulate import tabulate

if __name__ == '__main__':

    spark = SparkSession.builder.master("local").appName("test").getOrCreate()


    myManualSchema = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("item_id", IntegerType(), True),
        StructField("rating", IntegerType(), True, metadata={"Hello": "World"}),
        StructField("timestamp", IntegerType(), True)
    ])

    # schemaString="user_id item_id rating timestamp"
    # fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
    # schema= StructType(fields)

    df = spark.read.option("header", "true").schema(myManualSchema)\
        .option("delimiter","\t").csv("file:///C:/Ketan - Personal/Self Learning/Big Data/Spark/SparkCourse/ml-100k/u.data")
    df.createOrReplaceTempView("movierating")
    results= spark.sql("""Select rating, count(1) from movierating group by rating order by rating""")
    print(tabulate(results.collect()))

