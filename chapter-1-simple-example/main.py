from pyspark.sql import SparkSession
import os

current_dir = os.path.dirname(os.path.realpath(__file__))
data_file_name = "../csv_files/data.csv"
data_file_path = os.path.join(current_dir, data_file_name)
spark_session = SparkSession.builder.appName("SimpleCSVReaderExample").master("spark://14.225.254.77:7077").getOrCreate()

def read_csv_spark(file_path):
    data_frame = spark_session.read.csv(path=file_path, header=True, inferSchema=True)
    data_frame.show(5)
    spark_session.stop()

if __name__ == "__main__":
    read_csv_spark(data_file_path)