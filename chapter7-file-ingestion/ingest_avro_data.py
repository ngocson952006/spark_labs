import os
import logging

from pyspark.sql import SparkSession

file_name = "../csv_files/userdata.avro"
current_dir = os.path.dirname(__file__)
file_path = os.path.join(current_dir, file_name)

# Step 2: Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

spark_master_url = os.getenv('SPARK_MASTER_URL', "local[*]")

def ingest_avro_data():
    logger.info("Chapter 7 - Ingest AVRO file with options")
    spark_session = (SparkSession.builder.appName("AvroIngestInspect")
                     .config("spark.jars.packages",
                             "org.apache.spark:spark-avro_2.12:3.4.1")
                     .getOrCreate())

    logger.info("Start reading AVRO file. Using Spark version: {}".format(spark_session.version))
    df = spark_session.read.format("avro").load(file_path)

    logging.info("Show the file schema information")
    df.printSchema()

    logger.info("Show 5 first rows")
    df.show(5)

    logger.info("End Chapter 7 - Ingest AVRO file with options")

if __name__ == "__main__":
    ingest_avro_data()
