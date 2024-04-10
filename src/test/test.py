from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]") \
        .appName("lokesh_spark2")\
        .config("spark.driver.extraClassPath", "C:\\my_sql_jar\\mysql-connector-java-8.0.26.jar") \
        .getOrCreate()

spark.read.text("")
