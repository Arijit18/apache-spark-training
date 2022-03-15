import sys

from pyspark.sql import *
from lib.logger import Log4J
from lib.utils import get_spark_app_config, load_survey_df, count_by_country
from pyspark import SparkConf

if __name__ == '__main__':
    # spark = SparkSession.builder\
    #     .appName("HelloSpark")\
    #     .master("local[3]")\
    #     .getOrCreate()

    # conf = SparkConf()
    # conf.set("spark.app.name", "HelloSpark")
    # conf.set("spark.master", "local[3]")
    #
    # spark = SparkSession.builder\
    #     .config(conf=conf)\
    #     .getOrCreate()

    conf = get_spark_app_config()
    spark = SparkSession.builder\
        .config(conf=conf)\
        .getOrCreate()

    logger = Log4J(spark)
    logger.info(f"Starting {spark.conf.get('spark.app.name')}")
    # conf_out = spark.sparkContext.getConf()
    # logger.debug(conf_out.toDebugString())
    survey_df: DataFrame = load_survey_df(spark, sys.argv[1])

    partitioned_df = survey_df.repartition(2)

    count_df: DataFrame = count_by_country(partitioned_df)

    logger.debug(count_df.collect())

    input("Continue..")
    logger.info(f"Finishing {spark.conf.get('spark.app.name')}")
    spark.stop()
