from collections import namedtuple

from pyspark import SparkConf, SparkContext
from pyspark.sql import *
from lib.logger import Log4J
import sys

SurveyRecord = namedtuple("SurveyRecord", ["Age", "Gender", "Country", "State"])

if __name__ == '__main__':
    conf = SparkConf()

    conf.setMaster("local[3]").setAppName("HelloRDD")

    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    # spark_context = SparkContext(conf=conf)

    spark_context = spark.sparkContext
    logger = Log4J(spark)

    if len(sys.argv) != 2:
        logger.error("Usage: HelloRDD <filename>")
        sys.exit(1)

    linesRDD = spark_context.textFile(sys.argv[1])
    partitionedRDD = linesRDD.repartition(2)

    colsRDD = partitionedRDD.map(lambda line: line.replace("\"", "").split(","))
    selectRDD = colsRDD.map(lambda cols: SurveyRecord(int(cols[1]), cols[2], cols[3], cols[4]))
    filteredRDD = selectRDD.filter(lambda r: r.Age < 40)
    kvRDD = filteredRDD.map(lambda r: (r.Country, 1))
    countRDD = kvRDD.reduceByKey(lambda v1, v2: v1+v2)

    colsList = countRDD.collect()
    for x in colsList:
        logger.info(x)
