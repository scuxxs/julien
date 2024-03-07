from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.functions import col, expr, concat, countDistinct, to_timestamp, year, count, to_date, month, \
    weekofyear
from pyspark.sql.types import StructType, ArrayType, StringType, StructField, IntegerType, BooleanType, FloatType

# Driver
spark = SparkSession \
    .builder \
    .master('local') \
    .appName('julien') \
    .getOrCreate()

pandas_df1 = pd.read_excel('./dataset/青年大学习.xlsx')
spark_df1 = spark.createDataFrame(pandas_df1)
