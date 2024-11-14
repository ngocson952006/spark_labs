import os
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

file_name = "../csv_files/chapter_4.csv"
current_dir = os.path.dirname(__file__)
file_path = os.path.join(current_dir, file_name)
mode = "normal"


def duplicated_df(df, times):
    built_df = df
    for i in range(times):
        built_df = built_df.union(df)
    return built_df


def experiment(file_path):
    print("--- Start experiment ---")
    t0 = int(round(time.time() * 1000))
    spark_session = SparkSession.builder.appName("SparkLaziness").master("local[*]").getOrCreate()
    t1 = int(round(time.time() * 1000))

    print(f"Create spark session took {t1 - t0} ms")

    df = spark_session.read.csv(
        path=file_path,
        header=True,
        inferSchema=True)

    t2 = int(round(time.time() * 1000))

    print(f"Load original dataframe took {t2 - t1} ms")

    df = duplicated_df(df, 60)
    print(f"Number of rows for now: {df.count()}")

    t3 = int(round(time.time() * 1000))

    print(f"Make larger dataframe took {t3 - t2} ms")

    # Step 4 - Cleanup. preparation
    df = df.withColumnRenamed("Lower Confidence Limit", "lcl") \
        .withColumnRenamed("Upper Confidence Limit", "ucl")

    t4 = int(round(time.time() * 1000))

    print(f"Clean up and prepare took {t4 - t3} ms")

    # Step 5, transform
    if mode != "noob":
        df = (df.withColumn("avg", F.expr("(lcl+ucl)/2"))
              .withColumn("lcl2", F.col("lcl"))
              .withColumn("ucl2", F.col("ucl")))
        if mode == "full":
            df = df.drop("avg", "ucl2", "lcl2")

    t5 = int(round(time.time() * 1000))
    print(f"Transformation step took {t5 - t4} ms")

    # Step 6 - Action
    df.collect()
    t6 = int(round(time.time() * 1000))
    print(f"Final action ................. {t6 - t5} ms")

    print("--- End experiment ---")

if __name__ == "__main__":
    print("Chapter 4: Spark Laziness")
    experiment(file_path)
