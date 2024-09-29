from pyspark.sql import SparkSession
from datetime import date
from dotenv import load_dotenv
import os

def loadUserDF(): 
    return spark.read.format("jdbc") \
        .option("url", DATA_SOURCE) \
        .option("driver", "oracle.jdbc.driver.OracleDriver") \
        .option("dbtable", "USERS") \ 
        .option("query", "select * from USERS where MODIFIED_DATE >= " +data_extract_date+ " and MODIFIED_DATE < " +todayDate)
        .option("user", USER)  \
        .option("password", PASSWORD) \ 
        .load()

def loadTransactionDF(): 
    return = spark.read.format("jdbc") \
        .option("url", DATA_SOURCE) \
        .option("driver", "oracle.jdbc.driver.OracleDriver") \
        .option("dbtable", "ORDERS") \ 
        .option("query", "select * from ORDERS where MODIFIED_DATE >= " +data_extract_date+ " and MODIFIED_DATE < " +todayDate)
        .option("user", USER)  \
        .option("password", PASSWORD) \ 
        .load()

def transform(): 
    userDf = loadUserDF
    transactionDf = loadTransactionDF
    return userDF.as("u") \
        .join(transactionDf.as("t"), userDf.id == transactionDf.user_id, "left") \
        .groupBy("u.id").agg( \
            sum("amount").as("total_amount"), \
            sum("u.id").as("number_of_transactions") \
        ) 

def load(df): 
    df.write.mode("overwrite").parquet(S3_ENPOINT + "/" + dataExtractDate)

def run():
    load_dotenv()

    appName = "ETL from Oracle to S3"
    master = "local"
    # Create Spark session
    spark = SparkSession.builder \
        .appName(appName) \
        .master(master) \
        .getOrCreate()

    dataExtractDate = date.today() - timedetla(days = 1)
    todayDate = date.today()

    DATA_SOURCE = os.getenv('DATA_SOURCE')
    USER = os.getenv('USER')
    PASSWORD = os.getenv('PASSWORD')
    S3_ENPOINT = os.getenv("S3_ENDPOINT")

    dailySummaryDf = transform()
    load(dailySummaryDf)