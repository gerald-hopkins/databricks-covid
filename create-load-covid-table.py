# Databricks notebook source
# MAGIC %md
# MAGIC # Create and load a raw table for covid data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create and Load the raw delta table

# COMMAND ----------

# file location and type
file_location = "/mnt/covid-data-gerald/covid_vaccine_statewise.csv"
file_type = "csv"
# csv options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","
# The applied options are for csv files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
    .option("inferSchema", infer_schema) \
    .option("header", first_row_is_header) \
    .option("sep", delimiter) \
    .load(file_location)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Renaming column headers in spark dataframe did not work. So, I had to manually edit the file to remove special characters.**

# COMMAND ----------

# df = df.withColumnRenamed("Updated On", "UpdatedOn") \
#       .withColumnRenamed("State", "State") \
#       .withColumnRenamed("Total Individuals Registered", "TotalIndividualsRegistered") \
#       .withColumnRenamed("Total Sessions Conducted", "TotalSessionsConducted") \
#       .withColumnRenamed("Total Sites", "TotalSites") \
#       .withColumnRenamed("First Dose Administered", "FirstDoseAdministered") \
#       .withColumnRenamed("Second Dose Administered", "SecondDoseAdministered") \
#       .withColumnRenamed("Male(Individuals Vaccinated)", "MaleIndividualsVaccinated") \
#       .withColumnRenamed("Female(Individuals Vaccinated)", "FemaleIndividualsVaccinated") \
#       .withColumnRenamed("Transgender(Individuals Vaccinated)", "TransgenderIndividualsVaccinated") \
#       .withColumnRenamed("Total Covaxin Administered", "TotalCovaxinAdministered") \
#       .withColumnRenamed("Total CoviShield Administered", "TotalCoviShieldAdministered") \
#       .withColumnRenamed("Total Individuals Vaccinated", "TotalIndividualsVaccinated") \
#       .withColumnRenamed("Total Doses Administered", "TotalDosesAdministered")

df.write.mode("overwrite").saveAsTable("covid_vaccine_statewise")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM covid_vaccine_statewise;
