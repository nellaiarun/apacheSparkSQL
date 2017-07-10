"""
@author: Arun T
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import *

def MovieRatingsFromHdfsToSparkSQL(spark):
    #Load Movie Lens Ratings data file i.e., u.data
    rows = sc.textFile("data/sql/ml-100k/u.data")
    #Data file is delimited by Tab
    columns = rows.map(lambda row: row.split("\t"))
    schemaString = "userId itemId rating timestamp"
    attributes = [StructField(attribute_name, StringType(), True) for attribute_name in schemaString.split()]
    schema = StructType(attributes)
    ratingsDF = spark.createDataFrame(columns, schema)
    ratingsDF.show()#It would display top 20 rows of Ratings data; we are using DataFrame
    #Register Ratings DataFrame as Table (Temp View)
    ratingsDF.createOrReplaceTempView("ratings")#It would now allow access the data using SparkSQL
    spark.sql("select count(*) from ratings").show()#Using SparkSQL display record count. It should display 100K.
    spark.sql("select * from ratings limit 10").show()#It'd display 10 records, difference though v r using SparkSQL now.
    #Following SparkSQL query creates a histogram of ratings. Evidently 4 star rated movies are high, 1 star being low.
    spark.sql("select rating, count(*) as total from ratings group by rating order by total desc").show()
    #Technically, this newly registered table (ratings) can be accessed from reporting tools like Tableau using Spark SQL.

if __name__ == "__main__":
    #spark = SparkSession.builder.getOrCreate() #uncomment this for spark-submit, as 'spark' exists in Jupyter.
    MovieRatingsFromHdfsToSparkSQL(spark)
    spark.stop()