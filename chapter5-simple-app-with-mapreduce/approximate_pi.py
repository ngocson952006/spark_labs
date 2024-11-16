from random import random
from pyspark.sql import SparkSession
from utils.time_utils import *


slices = 100
number_of_throws = 100000 * slices

def throw_darts(_):
    x = random() * 2 - 1
    y = random() * 2 - 1
    return 1 if x ** 2 + y ** 2 <= 1 else 0


def start():
    t0 =  current_time_ms()
    spark_session = (SparkSession.builder.appName("Approximate Pi")
                     .master("local[*]")
                     .getOrCreate())
    t1 = current_time_ms()
    print("Load spark session took: {} ms".format(t1 - t0))

    num_list = []

    for x in range(number_of_throws):
        num_list.append(x)

    increment_rdd  = spark_session.sparkContext.parallelize(num_list)
    t2 = current_time_ms()
    print("Initial dataframe built in {} ms".format(t2 - t1))

    # Start do mapping (do logic for each row of dataset)
    mapped_rdd = increment_rdd.map(throw_darts)
    t3 = current_time_ms()
    print("Mapping done in {} ms".format(t3 - t2))

    # Start reduce
    # reduced_rdd_result = mapped_rdd.reduce(add) # this is sum count from map result of whole dataset
    reduced_rdd_result = mapped_rdd.reduce(lambda x, y: x + y)
    t4 = current_time_ms()

    print("Analyzing result in {} ms".format(t4 - t3))

    print("Pi is roughly {}".format(4.0 * reduced_rdd_result / number_of_throws))


if __name__ == '__main__':
    start()