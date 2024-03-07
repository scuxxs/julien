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

# schema = StructType([
#     StructField("name", StringType(), True),
#     StructField("college", StringType(), True),
#     StructField("major", StringType(), True),
#     StructField("node_name", StringType(), True),
#     StructField("late_level", IntegerType(), True),
#     StructField("cheat_level", IntegerType(), True),
#     StructField("poverty_level", IntegerType(), True),
#     StructField("politics_level", IntegerType(), True),
#     StructField("academy_level", IntegerType(), True),
#     StructField("mental_level", IntegerType(), True),
#     StructField("total_grade", IntegerType(), True)
# ])

# 创建DataFrame
# student_df = spark.createDataFrame([], schema)
# student_df = student_df.withColumn("college", lit("数据学院"))
# for field in schema.fields:
#     field_name = field.name
#     if field_name != "college":
#         student_df = student_df.withColumn(field_name, lit(0))

spark_df1 = spark_df1.select("sn").distinct()

spark_df1 = spark_df1.withColumnRenamed("sn", "id")
spark_df1 = spark_df1.withColumn("college",lit("数据学院").cast(StringType()))
spark_df1 = spark_df1.withColumn("major",lit("0").cast(StringType()))
spark_df1 = spark_df1.withColumn("node",lit("数据学院").cast(StringType()))
spark_df1 = spark_df1.withColumn("college",lit("数据学院").cast(StringType()))
spark_df1 = spark_df1.withColumn("college",lit("数据学院").cast(StringType()))
spark_df1 = spark_df1.withColumn("college",lit("数据学院").cast(StringType()))

# schema2 = StructType(spark_df1.schema.fields + schema)
# student_df = spark.createDataFrame(spark_df1.rdd, schema2)
# student_df = student_df.withColumn("college", lit("数据学院"))
# for field in schema.fields:
#     field_name = field.name
#     if field_name != "college":
#         student_df = student_df.withColumn(field_name, lit(0))

spark_df1.show(5)

# student_df.printSchema()