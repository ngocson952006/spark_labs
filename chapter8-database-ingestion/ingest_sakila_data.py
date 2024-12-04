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
    "dbtable": "actor",
    "numPartitions": "4",
    "partitionColumn": "actor_id",  # Column for partitioning
    "lowerBound": "1",  # Lower bound of the partition column
    "upperBound": "1000"  # Upper bound of the partition column
}

sql_query = "(SELECT * FROM actor WHERE last_name LIKE '%ZE%')"
connection_properties_with_query = {
    "url": connection_string,
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver",
    "dbtable": sql_query,
    "numPartitions": "4",
    "partitionColumn": "actor_id",  # Column for partitioning
    "lowerBound": "1",  # Lower bound of the partition column
    "upperBound": "1000"  # Upper bound of the partition column
}

join_sql_query = """ 
    (select actor.first_name, actor.last_name, film.title,
    film.description, film.film_id
    from actor, film_actor, film
    where actor.actor_id = film_actor.actor_id
    and film_actor.film_id = film.film_id)
    """
connection_properties_with_join_query = {
    "url": connection_string,
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver",
    "numPartitions": "4",
    "dbtable": join_sql_query,
    "partitionColumn": "film_id",  # Column for partitioning
    "lowerBound": "1",  # Lower bound of the partition column
    "upperBound": "1000"  # Upper bound of the partition column
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
        print("number of partitions: {}".format(df.rdd.getNumPartitions()))

        # combine with query
        print("--------------- Ingesting data with sql query ---------------")
        df_with_query = spark_session.read.format("jdbc").options(**connection_properties_with_query).load()
        df_with_query.show(5)
        print("number of partitions: {}".format(df_with_query.rdd.getNumPartitions()))

        # with join query
        df_with_join_query = spark_session.read.format("jdbc").options(**connection_properties_with_join_query).load()
        df_with_join_query.show(5)
        print("number of partitions: {}".format(df_with_join_query.rdd.getNumPartitions()))

    except Exception as e:
        print(f"Ingested data into database failed: {e}")
    finally:
        spark_session.stop()
        print("--------------- End ingestion ---------------")


if __name__ == "__main__":
    print("------- Analyze data from database -------")
    ingest_data_from_database()
