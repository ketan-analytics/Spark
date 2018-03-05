from pyspark import SparkConf,SparkContext

def parseline(line):
    linesplit=line.split(",")
    customerid=str(linesplit[0])
    amount=float(linesplit[2])
    return (customerid,amount)



conf=SparkConf().setMaster("local").setAppName("CustomerSpent")
sc=SparkContext(conf=conf)

readfile=sc.textFile("file:///C:/Ketan - Personal/Self Learning/Big Data/Spark/SparkCourse/customer-orders.csv")
rddparsed=readfile.map(parseline)
rddsum=rddparsed.reduceByKey(lambda x,y:x+y)
rddsorted=rddsum.map(lambda x: (x[1], x[0])).sortByKey()
results= rddsorted.collect()

for result in results:
     customerid=result[1]
     amount=result[0]
     print(f'{customerid}:{amount:.2f}')


