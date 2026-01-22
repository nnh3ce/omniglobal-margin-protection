from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, StructType, StructField

file_path = "/Volumes/omniglobal__margin_protection/omni_bronze/ingest_files/exchange_rates"

schema = StructType(
    [
        StructField("1._From_Currency_Code", StringType(), True),
        StructField( "2._From_Currency_Name", StringType(), True),
        StructField("3._To_Currency_Code",StringType(), True),
        StructField("4._To_Currency_Name", StringType(), True),
        StructField("5._Exchange_Rate", StringType(), True),
        StructField("6._Last_Refreshed", StringType(), True),
        StructField("7._Time_Zone", StringType(), True),
        StructField("8._Bid_Price", StringType(), True),
        StructField( "9._Ask_Price", StringType(), True)
    ]      
)

@dp.table(comment="Raw exchange rate data for Omni Global")

def exchange_rates_raw():

   df = spark.readStream.format("cloudFiles").schema(schema).option("cloudFiles.format","json").option("cloudFiles.inferColumnTypes", "true").load(file_path)

   return df