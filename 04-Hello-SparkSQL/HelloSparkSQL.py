import sys

from pyspark.sql import SparkSession
from pyspark import SparkConf
from lib.logger import Log4J

if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("HelloSparkSQL") \
        .getOrCreate()

    logger = Log4J(spark)

    if len(sys.argv) != 2:
        logger.error("Usage: HelloRDD <filename>")
        sys.exit(1)

    surveyDF = spark\
        .read\
        .option("inferSchema", "true")\
        .option("header", "true")\
        .csv(sys.argv[1])

    surveyDF.createOrReplaceTempView("survey_tbl")

    countDF = spark.sql("select Country, count(1) as count from survey_tbl where Age < 40 group by Country")
    countDF.show()
