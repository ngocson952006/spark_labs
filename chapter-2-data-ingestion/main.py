import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

current_dir = os.path.dirname(os.path.realpath(__file__))
csv_data_file_name = "../csv_files/data.csv"
jdbc_dataset_driver_file_name = "../libs/postgresql-42.7.4.jar"
jdbc_dataset_driver_file_path = os.path.join(current_dir, jdbc_dataset_driver_file_name)
csv_data_file_path = os.path.join(current_dir, csv_data_file_name)

# define database connection properties
connection_string = "jdbc:postgresql://localhost:5432/spark_labs"
connection_properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

def ingest_csv_data_into_database(csv_file_path):
    """
    Ingest raw data from csv file to database.
    :param csv_file_path: Path to the CSV file to be ingested.
    :return: None
    """
    spark_session = (SparkSession.builder.appName("SimpleCSVReaderExample")
                     .config("spark.jars", jdbc_dataset_driver_file_path)
                     .master("local[*]")
                     .getOrCreate())
    try:
        print("--------------- Start ingestion ---------------")
        df = spark_session.read.csv(path=csv_file_path, header=True, inferSchema=True)

        # Transforming data, for this example, we will add new column call full_name
        df = df.withColumn("full_name", F.concat(F.col("first_name"), F.lit(", "), F.col("last_name")))

        # show fist 5 rows of data
        df.show(5)

        # Start ingesting data
        df.write.jdbc(
            url=connection_string,
            table="chapter2_data",
            mode="overwrite",
            properties=connection_properties)
    except Exception as e:
        print(f"Ingested data into database failed: {e}")
    finally:
        spark_session.stop()
        print("--------------- End ingestion ---------------")


if __name__ == "__main__":
    ingest_csv_data_into_database(csv_data_file_path)