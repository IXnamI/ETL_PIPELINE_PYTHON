# ETL Pipeline in Python
A simple daily ETL Pipeline written in Python 

## Description
It loads data from Oracle, filters for the data that was changed at date T-1, transform it and then save it to the daily partition in S3. The task is automated using Apache Airflow which run the script daily at 0 3 * * *. 

The project only contains code and is not runnable as is. Hadoop's configurations are needed to write to S3 without adding properties to the Spark app. 
