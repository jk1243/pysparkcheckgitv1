import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]") \
                    .appName('tp_proj_bloco') \
                    .getOrCreate()

pysparkDF = spark.read.csv("titanic.csv",
                           header=True,
                           inferSchema=True,
                           sep=",")
pysparkDF.show(5)
