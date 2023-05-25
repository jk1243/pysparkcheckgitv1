# Import the required modules
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Create a Spark session
spark = SparkSession.builder.appName(
  "Replit Pyspark UDF example").getOrCreate()


# Define the Python function
def greet(name):
  return "Hello, " + name + "!"


# Define the Pyspark UDF
greet_udf = udf(lambda x: greet(x), StringType())

# Create a Pyspark dataframe
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
df = spark.createDataFrame(data, ["name", "age"])

# Apply the UDF to the Pyspark dataframe
result = df.withColumn("greeting", greet_udf(df.name))

# Show the result
result.show()

# Stop the Spark session
spark.stop()
