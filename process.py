from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.functions import col, expr, concat, countDistinct, to_timestamp, year, count, to_date, month, \
    weekofyear, lit
from pyspark.sql.types import StructType, ArrayType, StringType, StructField, IntegerType, BooleanType, FloatType

# Driver
spark = SparkSession \
    .builder \
    .master('local') \
    .appName('julien') \
    .getOrCreate()

pandas_df1 = pd.read_excel('./dataset/青年大学习.xlsx')
spark_df1 = spark.createDataFrame(pandas_df1)

schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("college", StringType(), True),
    StructField("major", StringType(), True),
    StructField("node_name", StringType(), True),
    StructField("late_level", IntegerType(), True),
    StructField("cheat_level", IntegerType(), True),
    StructField("poverty_level", IntegerType(), True),
    StructField("politics_level", IntegerType(), True),
    StructField("academy_level", IntegerType(), True),
    StructField("mental_level", IntegerType(), True),
    StructField("total_grade", IntegerType(), True)
])

# 创建DataFrame
student_df = spark.createDataFrame([], schema)
student_df = student_df.withColumn("college", lit("数据学院"))
student_df = student_df.withColumn("late_level", lit(0))
student_df = student_df.withColumn("cheat_level", lit(0))
student_df = student_df.withColumn("poverty_levle", lit(0))
student_df = student_df.withColumn("polo", lit(0))


student_df.printSchema()