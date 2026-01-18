from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructType, StructField

file_path = "/Volumes/omniglobal__margin_protection/omni_bronze/ingest_files" #how do I point to Git Repo instead so pipeline doesn't fail?

schema = StructType(
    [
        StructField("transaction_id", IntegerType(), True),
        StructField("timestamp", StringType(), True),
        StructField("product_code",StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("local_price", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("store_id", IntegerType(), True),
        StructField("customer_loyalty_id", IntegerType(), True)
    ]
)

@dp.table(comment="Raw order data for Omni Global" )

def ingest_raw():
    return (spark.readStream.format("cloudFiles").schema(schema).option("cloudFiles.format","json").load(file_path))
