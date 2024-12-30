#%%
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd
import base64

# Decoded SQL query
decoded_query = base64.b64decode('c2VsZWN0ICogZnJvbSBpbnZvaWNlX2J5X2FjY291bnQ=').decode('utf-8')

#%% md
# Define file paths
#%%
current_dir = os.getcwd()
invoices_files_names = '../csv_files/invoices/invoice-*.json'
invoices_files_path = os.path.join(current_dir, invoices_files_names)
#%% md
# Start ingest data into Spark
#%%
spark = SparkSession.builder.appName('JSON data transformation').getOrCreate()
invoices_df = spark.read.format('json').option('multiline', True).load(invoices_files_path)
invoices_df.show(10)
#%% md
# Analytic invoice amount
#%%
invoice_amount_df = invoices_df.select(F.col("totalPaymentDue.*"))
invoice_amount_df.show()
#%%
invoice_by_account = invoices_df.select(F.col("accountId"), F.explode(F.col("referencesOrder")).alias("order"))
invoice_by_account = invoice_by_account \
    .withColumn("type", F.col("order.orderedItem.@type")) \
    .withColumn("description", F.col("order.orderedItem.description")) \
    .withColumn("name", F.col("order.orderedItem.name"))
invoice_by_account.show(10, False)

invoice_by_account.createOrReplaceTempView("invoice_by_account")
spark.stop()