from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType  # <- adjust types

schema = StructType([
    StructField("order_id", StringType()),
    StructField("product",  StringType()),
    StructField("quantity", IntegerType()),
    StructField("price",    DoubleType()),
    # add all real fields here to avoid expensive inference
])

spark = (
    SparkSession.builder
    .appName("LoadOrderData")
    .config("spark.driver.memory", "4g")       # bump if you can
    .config("spark.executor.memory", "4g")     # local mode uses driver; harmless to set
    .config("spark.sql.shuffle.partitions", "200")
    .getOrCreate()
)

url = "https://data-architect-test-source.s3-sa-east-1.amazonaws.com/order.json.gz"

df = spark.read.schema(schema).json(url)  # avoid schema inference
df = df.repartition(200)                  # parallelize downstream work (after read)

print(df.count())                         # force a job to verify load
df.show(5, truncate=False)

spark.stop()
