from airflow import DAG
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
        'pyspark_example',
        default_args=default_args,
        description='A DAG that runs PySpark code',
        schedule_interval=None,  # Run manually for simplicity
        start_date=datetime(2023, 10, 20),
        catchup=False,
) as dag:
    # Define a Python function to execute PySpark code
    def run_pyspark():
        # Initialize SparkSession
        spark = SparkSession.builder \
            .appName("PySpark Example") \
            .master("local[*]") \
            .getOrCreate()

        # Example: Create a DataFrame and show it
        data = [("John", 30), ("Jane", 25), ("Sam", 35)]
        columns = ["Name", "Age"]
        df = spark.createDataFrame(data, columns)
        df.printSchema()
        df.show()  # Print the DataFrame to the console/logs

        # Stop the Spark session
        spark.stop()


    # Create a PythonOperator to run the PySpark function
    pyspark_task = PythonOperator(
        task_id='pyspark_task',
        python_callable=run_pyspark,
    )

    pyspark_task  # Define the task (no dependencies in this example)
