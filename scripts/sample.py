# Import the required modules
from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession
.builder
.appName(‘spark-read-from-bigquery’)
.getOrCreate()

# Specify the BigQuery project, dataset and table
project = ‘YOUR_PROJECT_ID’ dataset = ‘YOUR_DATASET_ID’ table = ‘YOUR_TABLE_ID’

# Read the BigQuery table into a Spark dataframe
df = spark.read.format(‘bigquery’)
.option(‘project’, project)
.option(‘dataset’, dataset)
.option(‘table’, table)
.load()

# Display the schema and some rows of the dataframe
df.printSchema() df.show()

# Stop the Spark session
spark.stop()
