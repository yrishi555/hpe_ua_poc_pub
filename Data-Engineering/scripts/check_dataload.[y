# Import necessary libraries
from pyspark.sql import SparkSession
from py4j.java_gateway import java_import
java_import(spark._sc._jvm, "org.apache.spark.sql.api.python.*")
from delta.tables import DeltaTable
# Define the folder path
delta_path = "s3a://truepoc-bkt-raw/test/"
# Connects to EzPresto over SSL, executes a SQL query, and loads the result into a Spark DataFrame
# Read Data from Delta
df = spark.read.format("delta").load(delta_path)
df.show(5)