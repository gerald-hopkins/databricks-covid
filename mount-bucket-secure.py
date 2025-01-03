# Databricks notebook source
# MAGIC %md
# MAGIC **Uploaded credentials file for AWS user employee to FileStore via Legacy option in Databrick UI**. <br>
# MAGIC **This will allow me to access the S3 bucket (once mounted to dbfs) from Databricks in a secure manner**

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables")

# COMMAND ----------

from pyspark.sql.functions import *
import urllib

# COMMAND ----------

# define file type
file_type = "csv"
# whether the file has a header
first_row_is_header = True
# delimiter used in file
delimiter = ","
# read csv file into spark dataframe
df_aws_cred = spark.read.format(file_type) \
.option("header", first_row_is_header) \
.option("sep", delimiter) \
.load("/FileStore/tables/employee_accessKeys.csv")

# COMMAND ----------

# get aws access keys from the spark dataframe
ACCESS_KEY = df_aws_cred.select("Access key ID").collect()[0]['Access key ID']
SECRET_KEY = df_aws_cred.select("Secret access key").collect()[0]['Secret access key']
# encode the secret key
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe='')

# COMMAND ----------

# mount the aws bucket to the dbfs
AWS_S3_BUCKET = "covid-data-gerald"
# mount name for the bucket
MOUNT_NAME = "/mnt/covid-data-gerald"
# source url
SOURCE_URL = "s3n://{0}:{1}@{2}".format(ACCESS_KEY,SECRET_KEY,AWS_S3_BUCKET)
# mount the drive
dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)

# COMMAND ----------

# MAGIC %md **Verify that files are available in the mounted directory**

# COMMAND ----------

# MAGIC %fs ls "/mnt/covid-data-gerald"
