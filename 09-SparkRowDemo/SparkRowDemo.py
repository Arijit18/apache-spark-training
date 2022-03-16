from pyspark.sql import SparkSession, Row, DataFrame
from pyspark.sql.functions import to_date
from pyspark.sql.types import StructType, StructField, StringType

from lib.logger import Log4J


def to_date_df(df, date_format, field):
    return df.withColumn(field, to_date(field, date_format))


if __name__ == '__main__':
    spark = SparkSession\
        .builder\
        .master("local[3]")\
        .appName("SparkRowDemo")\
        .getOrCreate()

    logger = Log4J(spark)

    schema = StructType(
        [
            StructField("ID", StringType()),
            StructField("EventDate", StringType())
        ]
    )

    my_rows = [Row("123", "04/05/2020"), Row("124", "4/5/2020"), Row("125", "04/5/2020"), Row("126", "4/05/2020")]
    my_rdd = spark.sparkContext.parallelize(my_rows, 2)
    my_df = spark.createDataFrame(my_rdd, schema)
    my_df.show()
    my_df.printSchema()

    new_df = to_date_df(my_df, "M/d/y", "EventDate")
    new_df.show()
    new_df.printSchema()
