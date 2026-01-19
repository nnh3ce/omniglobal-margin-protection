from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, StructType, StructField

#current_user = spark.sql("SELECT current_user()").first()[0] #dynamically retrieve user email

file_path = "/Workspace/Users/nnh3ce@gmail.com/omniglobal-margin-protection"

#"/Volumes/omniglobal__margin_protection/omni_bronze/ingest_files"  #manually uploaded

schema = StructType(
    [
        StructField("1. From_Currency Code", StringType(), True),
        StructField( "2. From_Currency Name", StringType(), True),
        StructField("3. To_Currency Code",StringType(), True),
        StructField("4. To_Currency Name", StringType(), True),
        StructField("5. Exchange Rate", StringType(), True),
        StructField("6. Last Refreshed", StringType(), True),
        StructField("7. Time Zone", StringType(), True),
        StructField("8. Bid Price", StringType(), True),
        StructField( "9. Ask Price", StringType(), True)
    ]
       
)

@dp.table(comment="Raw exchange rate data for Omni Global" )

def ingest_raw():
    return (spark.readStream.format("cloudFiles").schema(schema).option("cloudFiles.format","json").load(file_path))
