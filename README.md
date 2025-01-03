# databricks-covid
This repo contains my work on India's covid dataset in Databricks.

This demonstrates the ability to:

- mount an S3 bucket to Databricks dbfs (mount-bucket-secure.py)
- mount the bucket in a secure way such that AWS account credentials are not revealed in the source code or notebook (mount-bucket-secure.py)
- load spark dataframe from covid_vaccine_statewise.csv mounted file (create-load-covid-table.py)
- load data from spark dataframe into delta table in Unity Catalog (create-load-covid-table.py)
- use SQL in Databricks notebooks to profile data in raw table covid_vaccine_statewise (analyze-covid-table.py)
    * Analysis: Sum of vaccines given in all of India thru 4/21/21
    * Analysis: Sum of vaccines given in each Indian State thru 4/21/21
    * Analysis: Sum of vaccines given to each gender in all of India thru 4/21/21
