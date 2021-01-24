from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)


def parseLine(line):
    fields = line.split(',')
    custID = int(fields[0])
    amount = float(fields[2])
    return (custID, amount)

lines = sc.textFile("file:////Users/apple/data_oil/taming/customer-orders.csv")

rdd = lines.map(parseLine)  # map creates new rdd
totalByID = rdd.reduceByKey(lambda y, x: (x + y))

flipped = totalByID.map(lambda x: (x[1],x[0]))
totalByIDSorted = flipped.sortByKey()

results = totalByIDSorted.collect()

for result in results:
    print(result)