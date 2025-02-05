from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# Initialize Spark Session
spark = SparkSession.builder.appName('DeezerDataAnalysis').getOrCreate()

# Define schema for Deezer data
schema = StructType([
    StructField('user_id', IntegerType(), True),
    StructField('song_id', IntegerType(), True),
    StructField('timestamp', TimestampType(), True),
    StructField('country', StringType(), True),
    StructField('platform', StringType(), True)
])

# Load sample data
data_path = 'data/deezer_sample_data.csv'
df = spark.read.csv(data_path, schema=schema, header=True)

# Transform data: Add a 'year' column from the 'timestamp'
df_transformed = df.withColumn('year', year(col('timestamp')))

# Show transformed data
df_transformed.show()

# Save transformed data as a Delta table
df_transformed.write.format('delta').mode('overwrite').save('/mnt/blob-storage/deezer-data/silver')

spark.stop()

