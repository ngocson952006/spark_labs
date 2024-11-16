import os
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

file_name = "../csv_files/chapter7_books.csv"
current_dir = os.path.dirname(__file__)
file_path = os.path.join(current_dir, file_name)

# Step 2: Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

spark_master_url = os.getenv('SPARK_MASTER_URL', "local[*]")

def ingest_books():
    logger.info("Chapter 7 - Ingest CSV file with options")
    spark_session = (SparkSession.builder.appName("CSVIngestInspect")
                     .getOrCreate())

    logger.info("Start reading CSV file. Using Spark version: {}".format(spark_session.version))
    df = (spark_session.read.format("csv")
          .option("header", "true")
          .option("multiline", True)
          .option("sep", ";")
          .option("quote", "*")
          .option("dateFormat", "MM/dd/yyyy")
          .option("inferSchema", True)
          .load(file_path))
    logging.info("Show the file schema information")
    df.printSchema()
    logger.info("Show 5 first rows")
    df.show(5)

    logger.info("End Chapter 7 - Ingest CSV file with options")

if __name__ == "__main__":
    ingest_books()
