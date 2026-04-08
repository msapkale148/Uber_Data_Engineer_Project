from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

#Rides schema
df= spark.sql("select * from uber.bronze.bulk_rides")
rides_schema = df.schema

#Empty Streaming Table
dp.create_streaming_table("stg_rides")

#Bulk/Initial Load
@dp.append_flow(
  target = "stg_rides"
)
def rides_bulk():
    df = spark.readStream.table("bulk_rides")   #Initial Load                
    return df

#Streaming Load
@dp.append_flow(
  target = "stg_rides"
)
def rides_stream():
    df = spark.readStream.table("rides_raw") #Streaming Load   
    df_parsed = df.withColumn("parsed_rides", from_json(col("rides"), rides_schema))\
    .select("parsed_rides.*")               
    return parsed_df







