from pyspark import pipelines as dp
from pyspark.sql.function import *
from pyspark.sql.type import DoubleType, IntegerType, StringType, StructType, StructField

file_path = "C:\Users\pa\Documents\data_project_1\orders"

schema = StructType(
    [
        StructField("transaction_id", IntegerType, True),
        StructField("timestamp", StringType, True),
        StructField("product_code",StringType, True),
        StructField("product_name", StringType, True),
        StructField("brand", StringType, True),
        StructField("currency", StringType, True),
        StructField("local_price", IntegerType, True),
        StructField("quantity", IntegerType, True),
        StructField("store_id", IntegerType, True),
        StructField("customer_loyalty_id", IntegerType, True)
    ]
)

@dp.table(comment="Raw order data for Omni Global" )

def ingest_raw():
    return (spark.readStream.format("cloudFiles").schema(schema).option("cloudFiles.format","json").load(file_path))
