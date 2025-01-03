# Databricks notebook source
# MAGIC %md
# MAGIC # Analyze covid_vaccine_statewise Table

# COMMAND ----------

# MAGIC %md
# MAGIC ## SELECT all records (all covid vaccines given in all States of India in tracking period)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM covid_vaccine_statewise;

# COMMAND ----------

# MAGIC %md
# MAGIC **Each State has a different record for each day of the tracking period.** <br>
# MAGIC **And each day's record is a running total of the metrics like doses.** <br>
# MAGIC **I need to determine last report date in the dataset for each State.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## SELECT Min and Max dates updated for each State

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT State, MIN(UpdatedOn), MAX(UpdatedOn) FROM covid_vaccine_statewise GROUP BY State;

# COMMAND ----------

# MAGIC %md
# MAGIC **every State has a first record on 1/16/21 and a final record on 4/21/21 <br>
# MAGIC So, I need to total all the States' records from 4/21/21 to get the totals during the period for ALL States**

# COMMAND ----------

# MAGIC %md
# MAGIC ## SELECT Total Vaccines Given in ALL States in India through 4/21/21

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SUM(TotalDosesAdministered) AS `Total Doses Administered in ALL Indian States through 4/21/21`
# MAGIC FROM covid_vaccine_statewise
# MAGIC WHERE UpdatedOn = '2021-04-21';

# COMMAND ----------

# MAGIC %md
# MAGIC ## SELECT Total Doeses Administered By EACH Indian State thru 4/21/21

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT State, TotalDosesAdministered 
# MAGIC FROM covid_vaccine_statewise 
# MAGIC WHERE UpdatedOn = '2021-04-21' AND State NOT IN ("India")
# MAGIC ORDER BY TotalDosesAdministered DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## SELECT TOP 10 States Administering the MOST Doses by 4/21/21

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT State, TotalDosesAdministered
# MAGIC FROM covid_vaccine_statewise
# MAGIC WHERE UpdatedOn = '2021-04-21' AND State NOT IN ("India")
# MAGIC ORDER BY TotalDosesAdministered DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## SELECT Doses by Gender in ALL India thru 4/21/21

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SUM(MaleIndividualsVaccinated) as `Total Male Doses`,
# MAGIC   SUM(FemaleIndividualsVaccinated) as `Total Female Doses`,
# MAGIC   SUM(TransgenderIndividualsVaccinated) as `Total Transgender Doses`
# MAGIC FROM covid_vaccine_statewise
# MAGIC WHERE UpdatedOn = '2021-04-21' AND State NOT IN ("India");
