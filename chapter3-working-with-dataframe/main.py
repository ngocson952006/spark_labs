import json
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

current_dir = os.path.dirname(os.path.realpath(__file__))
csv_data_file_name = "../csv_files/restaurants_2.csv"
json_data_file_name = "../csv_files/restaurants_1.json"
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


def ingest_csv_file_data(spark_session, csv_file_path):
    """
    :param spark_session: A Spark session object used for DataFrame creation and operations.
    :param csv_file_path: Path to the CSV file to be ingested.
    :return: Transformed Spark DataFrame with ingested and processed data from the CSV file.
    """
    # Start to tranform and do inspection
    df = spark_session.read.csv(path=csv_file_path, header=True, inferSchema=True)

    # Transforming data, for this example, we will add new column call full_name
    # df = df.withColumn("full_name", F.concat(F.col("first_name"), F.lit(", "), F.col("last_name")))
    logging.warning("Start inferring data with schema")

    # show fist 5 rows of data
    #df.show(5)

    logging.warning("Original schema:")
    df.printSchema()

    # Start to transform columns
    df = df.withColumn("county", F.lit("Wake")) \
        .withColumnRenamed("HSISID", "dataset_id") \
        .withColumnRenamed("NAME", "name") \
        .withColumnRenamed("ADDRESS1", "address1") \
        .withColumnRenamed("ADDRESS2", "address2") \
        .withColumnRenamed("CITY", "city") \
        .withColumnRenamed("STATE", "state") \
        .withColumnRenamed("POSTALCODE", "zip") \
        .withColumnRenamed("PHONENUMBER", "tel") \
        .withColumnRenamed("RESTAURANTOPENDATE", "date_start") \
        .withColumnRenamed("FACILITYTYPE", "type") \
        .withColumnRenamed("X", "geox") \
        .withColumnRenamed("Y", "geoy") \
        .withColumn("date_end", F.lit(None)) \
        .drop("OBJECTID", "PERMITID", "GEOCODESTATUS")

    df = df.withColumn("id",
                       F.concat(F.col("state"), F.lit("_"),
                                F.col("county"), F.lit("_"),
                                F.col("dataset_id")))

    logging.warning("Transformed schema:")
    df.show(5)
    df.printSchema()

    # Inspect Schema in JSON format
    # schema_of_json = df.schema.json()
    # parsed_schema_json = json.loads(schema_of_json)
    # logging.warning(f"Schema in JSON format: {json.dumps(parsed_schema_json, indent=2)}")
    #
    # logging.warning(f"Total of records: {df.count()}")

    logging.warning("End inferring data with schema")

    # logging.warning("Looking at partitions information")
    # number_of_partitions = df.rdd.getNumPartitions()
    # logging.warning(f"Number of partitions: {number_of_partitions}")
    #
    # df = df.repartition(4)

    logging.warning("Partition count after repartition: {}".format(df.rdd.getNumPartitions()))

    return df


def ingest_json_data_into_database(spark_session, json_file_path):
    logging.warning("Start to ingest JSON data")
    df = spark_session.read.json(json_file_path)
    df.printSchema()

    logging.warning("We have {} records.".format(df.count))
    drop_cols = ["fields", "geometry", "record_timestamp", "recordid", "datasetid"]
    df = df.withColumn("county", F.lit("Durham")) \
        .withColumn("dataset_id", F.col("fields.id")) \
        .withColumn("name", F.col("fields.premise_name")) \
        .withColumn("address1", F.col("fields.premise_address1")) \
        .withColumn("address2", F.col("fields.premise_address2")) \
        .withColumn("city", F.col("fields.premise_city")) \
        .withColumn("state", F.col("fields.premise_state")) \
        .withColumn("zip", F.col("fields.premise_zip")) \
        .withColumn("tel", F.col("fields.premise_phone")) \
        .withColumn("date_start", F.col("fields.opening_date")) \
        .withColumn("date_end", F.col("fields.closing_date")) \
        .withColumn("type", F.split(F.col("fields.type_description"), " - ").getItem(1)) \
        .withColumn("geox", F.col("fields.geolocation").getItem(0)) \
        .withColumn("geoy", F.col("fields.geolocation").getItem(1)) \
        .drop(*drop_cols)

    df = df.withColumn("id", F.concat(F.col("state"), F.lit("_"),
                                      F.col("county"), F.lit("_"),
                                      F.col("dataset_id")))
    df.show(5)
    logging.warning("End to ingest JSON data")
    return df


def ingest_csv_data_into_database(csv_file_path, json_file_path):
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

        df_from_csv = ingest_csv_file_data(spark_session, csv_file_path)
        df_from_json = ingest_json_data_into_database(spark_session, json_file_path)

        # Start to union 2 df
        logging.warning("Start to union 2 df")
        final_df = df_from_csv.unionByName(df_from_json)
        final_df.show(5)
        final_df.printSchema()
        logging.warning("End to union 2 df")

        # Start ingesting data
        # df.write.jdbc(
        #     url=connection_string,
        #     table="chapter2_data",
        #     mode="overwrite",
        #     properties=connection_properties)
    except Exception as e:
        print(f"Ingested data into database failed: {e}")
    finally:
        spark_session.stop()
        print("--------------- End ingestion ---------------")


if __name__ == "__main__":
    ingest_csv_data_into_database(csv_data_file_path, json_data_file_name)
