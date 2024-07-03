from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test-spark").master("local[*]").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "5")

CSV_PATH = '2015-summary.csv'
spark_data = spark.read.option("inferSchema", "true").option("header", "true").csv(CSV_PATH, header=True)

spark_data.createOrReplaceTempView('spark_d`ata')
sqlWay = spark.sql("""
SELECT DEST_COUNTRY_NAME, count(1)
FROM spark_data
GROUP BY DEST_COUNTRY_NAME
""")

dataFrameWay = spark_data.groupBy("DEST_COUNTRY_NAME").count()
print(sqlWay.explain())
print(dataFrameWay.explain())
