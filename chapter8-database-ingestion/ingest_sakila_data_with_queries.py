import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

current_dir = os.path.dirname(os.path.realpath(__file__))
csv_data_file_name = "../csv_files/data.csv"
jdbc_dataset_driver_file_name = "../libs/postgresql-42.7.4.jar"
jdbc_dataset_driver_file_path = os.path.join(current_dir, jdbc_dataset_driver_file_name)
csv_data_file_path = os.path.join(current_dir, csv_data_file_name)

# define database connection properties
connection_string = "jdbc:postgresql://localhost:5432/sakila"
connection_properties = {
    "url": connection_string,
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver",
"dbtable": "actor"
}

def ingest_data_from_database():
    spark_session = (SparkSession.builder.appName("DatabaseIngestionExample")
                     .config("spark.jars", jdbc_dataset_driver_file_path)
                     .getOrCreate())
    try:
        print("--------------- Start ingestion ---------------")
        df = spark_session.read.format("jdbc").options(**connection_properties).load()
        # sort by a column
        df.orderBy(F.col("last_name"))
        df.show(5)
        df.printSchema()
    except Exception as e:
        print(f"Ingested data into database failed: {e}")
    finally:
        spark_session.stop()
        print("--------------- End ingestion ---------------")

if __name__ == "__main__":
    print("------- Analyze data from database -------")
    ingest_data_from_database()