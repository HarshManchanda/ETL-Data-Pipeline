from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, current_date
import pandas as pd

# Create SparkSession
spark = SparkSession.builder.appName("JSON to CSV").getOrCreate()

# Input and output paths
input_path = "/home/harsh/Dags_script/json data"
output_path = "/home/harsh/Dags_script/csv data"

# Read JSON files into a DataFrame
df = spark.read.option("multiLine", True).option("mode", "PERMISSIVE").json(input_path)

# Select the "address","latitude","longitude","days" column
df_new = df.select("address","latitude","longitude","days")

# Explode the "days" array column
df_new_exploded = df_new.selectExpr("address","latitude","longitude","inline(days)")

# Select required columns from exploded DataFrame
columns = [
    "datetime", "datetimeEpoch", "tempmax", "tempmin", "temp", "feelslikemax",
    "feelslikemin", "feelslike", "dew", "humidity", "precip", "precipprob",
    "precipcover", "snow", "snowdepth", "windgust", "windspeed",
    "winddir", "pressure", "cloudcover", "visibility", "solarradiation",
    "solarenergy", "uvindex", "severerisk", "sunrise", "sunriseEpoch", "sunset",
    "sunsetEpoch", "moonphase", "conditions", "description", "icon"
]

df_filtered = df_new_exploded.select("address","latitude","longitude",*columns)

df_filtered.toPandas().to_csv("/home/harsh/Dags_script/csv_data/mumbai.csv")


# Stop the SparkSession
spark.stop()