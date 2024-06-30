from pyspark.sql.session import SparkSession

def buildSparkSession(appName):
    spark=(SparkSession
           .builder
           .appName(appName)
           .master("local")
           .getOrCreate())
    return  spark