from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("OrderTotal").getOrCreate()

#  Create schema for csv file without header
schema = StructType([
    StructField("custID", IntegerType(), True),
    StructField("itemID", IntegerType(), True),
    StructField("amount", FloatType(), True),
])

#  Read the file as dataframe
df = spark.read.schema(schema).csv("file:////Users/apple/data_oil/taming/rdd/customer-orders.csv")
df.printSchema()

# selectAmount = df.select("custID", "amount")
#
# byCustID = selectAmount.groupBy("custID").sum("amount")
# byCustID.show()
#
# sortedSpenders = byCustID.withColumn("amount", func.round(func.col("sum(amount)"), 2)).select("custID", "amount").sort("amount")
# sortedSpenders.show()

#  OR

totalByCustomer = df.groupBy("custID").agg(func.round(func.sum("amount"), 2).alias("total_spent"))

totalByCustomerSorted = totalByCustomer.sort("total_spent")

totalByCustomerSorted.show(totalByCustomerSorted.count())

spark.stop()