from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructType, StructField

current_user = spark.sql("SELECT current_user()").first()[0]
#dynamically retrieve user email

file_path = "/Workspace/Users/nnh3ce@gmail.com/omniglobal-margin-protection/orders"
#"/Volumes/omniglobal__margin_protection/omni_bronze/ingest_files"  #manually uploaded

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




