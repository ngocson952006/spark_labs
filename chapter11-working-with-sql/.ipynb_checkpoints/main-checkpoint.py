import os
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField, StringType, DoubleType)
from pyspark.sql import functions as F

current_dir = os.path.dirname(os.path.realpath(__file__))
csv_data_file_name = "../csv_files/population.csv"
csv_data_file_path = os.path.join(current_dir, csv_data_file_name)


def ingest_data(csv_file_path):
    spark_session = (SparkSession.builder.appName("Analytic CSV with SQL")
                     .getOrCreate())
    try:
        print("--------------- Start ingestion ---------------")
        # define schema for df
        schema = StructType([
            StructField("geo", StringType(), True)
        ])

        # define more column
        for year in range(1980, 2011):
            schema.add(StructField(f"yr{year}", DoubleType(), True))

        df = spark_session.read.csv(path=csv_file_path, header=True, inferSchema=True, schema=schema)

        # delete used columns
        for year in range(1981, 2010):
            df = df.drop(F.col(f"yr{year}"))

        # Next, create a new column call "evolution" with formular from other columns
        df = df.withColumn("evolution", F.expr("round((yr2010 - yr1980) * 1000000)"))

        df.show(5, False)

        # print("Creating temp global view....")
        #
        # # from df, we will create a session scope temporary view as global
        # df.createOrReplaceTempView("geo_view")
        #
        # print("Created temp global view....")
        #
        # sql = """
        #     SELECT * from geo_view
        #     WHERE yr1980 < 1
        #     ORDER BY yr1980
        #     LIMIT 5
        # """
        #
        # small_countries = spark_session.sql(sql)
        # small_countries.show(10, False)
        #
        # # query with another session
        # print("Create another query from another session in same application")

        df.createTempView("geodata")

        # spark_session2 = spark_session.newSession()
        # sql_2 = """
        #     SELECT * from global_temp.geo_view
        #     WHERE yr1980 >= 1
        #     ORDER BY yr1980
        #     LIMIT 5
        # """
        # more_than_1_million_inhabitants = spark_session2.sql(sql_2)
        # more_than_1_million_inhabitants.show(10, False)

        # clean up the data
        clean_up_sql = """
        SELECT * FROM geodata
          WHERE geo is not null and geo != 'Africa'
           and geo != 'North America' and geo != 'World' and geo != 'Asia & Oceania'
           and geo != 'Central & South America' and geo != 'Europe' and geo != 'Eurasia'
           and geo != 'Middle East' order by yr2010 desc
        """

        clean_up_df = spark_session.sql(clean_up_sql)
        clean_up_df.createOrReplaceTempView("clean_up_view")

        # lost populations countries
        lost_population_sql = """
            SELECT * from clean_up_view
            WHERE evolution < 0
            ORDER BY evolution
        """
        lost_populations_countries_df = spark_session.sql(lost_population_sql)
        print("Lost population countries (Top 10):")
        lost_populations_countries_df.show(10, False)

    except Exception as e:
        print(f"Ingested data into database failed: {e}")
    finally:
        spark_session.stop()
        print("--------------- End ingestion ---------------")


if __name__ == "__main__":
    print("----- Start working with sql -----")
    ingest_data(csv_data_file_path)
    print("----- End working with sql -----")
