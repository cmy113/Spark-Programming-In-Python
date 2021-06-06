from pyspark.sql import *

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("SparkSQLTableDemo") \
        .enableHiveSupport() \
        .getOrCreate()

    logger = Log4j(spark)

    flightTimeParquetDF = spark.read \
        .format("parquet") \
        .load("dataSource/")

    spark.sql("CREATE DATABASE IF NOT EXISTS AIRLINE_DB")
    spark.catalog.setCurrentDatabase("AIRLINE_DB")

    # flightTimeParquetDF.write \
    #     .mode("overwrite") \
    #     .saveAsTable("flight_data_tbl")

    # Partition by ORIGIN, OP_CARRIER
    # flightTimeParquetDF.write \
    #     .mode("overwrite") \
    #     .partitionBy("ORIGIN", "OP_CARRIER") \
    #     .saveAsTable("flight_data_tbl")

    # Above implementation will cause too many partition
    # Lets use bucket instead, choose 5 buckets only, it will be computed based on hash and modulus
    # Since the unique combination of ORIGIN & OP_CARRIER will fall into same bucket
    # We will sort it as well
    flightTimeParquetDF.write \
        .format("csv") \
        .mode("overwrite") \
        .bucketBy(5, "ORIGIN", "OP_CARRIER") \
        .sortBy("OP_CARRIER", "ORIGIN") \
        .saveAsTable("flight_data_tbl")

    logger.info(spark.catalog.listTables("AIRLINE_DB"))
