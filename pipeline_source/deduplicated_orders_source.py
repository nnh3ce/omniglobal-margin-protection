from pyspark import pipelines as dp
from pyspark.sql.functions import *

orders_table = "ingest_raw"

@dp.table(comment ="A table of deduplicated_orders.")
def remove_duplicates():
    bronze_table =  dp.read_stream(orders_table)
    deduped = bronze_table.dropDuplicates(["transaction_id"])
    return deduped