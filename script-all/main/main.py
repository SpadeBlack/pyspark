from pyspark.sql import SparkSession
spark = SparkSession.builder.master('local').appName('YOLO').getOrCreate()

print("Spark object is created")
print("Number of partitions for shuffle changed to : " + str(spark.conf.get('spark.sql.shuffle.partitions')))
rdd = spark.sparkContext.textFile("F:/digi-world/py-spark/data-all/mainMain.txt")


# print(rdd.first())
print()
print(rdd.getNumPartitions())
# for i in rdd.collect():
#     print(i)
print(rdd.glom().map(len).collect())
spark.stop()
print("SPARK SESSION STOPPED!!")


