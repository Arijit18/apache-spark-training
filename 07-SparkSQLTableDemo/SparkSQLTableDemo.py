from pyspark.sql import *
from pyspark.sql.functions import spark_partition_id

from lib.logger import Log4J

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("07-08-SparkSQLTableDemo") \
        .enableHiveSupport() \
        .getOrCreate()

    logger = Log4J(spark)

    flightTimeParquetDF = spark\
        .read\
        .format("parquet")\
        .option("path", "datasource/")\
        .load()

    spark.sql("CREATE DATABASE IF NOT EXISTS AIRLINE_DB")
    spark.catalog.setCurrentDatabase("AIRLINE_DB")

    # flightTimeParquetDF\
    #     .write\
    #     .mode("overwrite")\
    #     .partitionBy("ORIGIN", "OP_CARRIER") \
    #     .saveAsTable("flight_data_tbl")

    flightTimeParquetDF \
        .write \
        .format("csv") \
        .mode("overwrite") \
        .bucketBy(5, "OP_CARRIER", "ORIGIN") \
        .sortBy("OP_CARRIER", "ORIGIN") \
        .saveAsTable("flight_data_tbl")

    logger.info(spark.catalog.listTables("AIRLINE_DB"))

