"""
@author: Arun T
"""
from pyspark.sql import SparkSession

def CreateAndLoadRatings(spark):
    spark.sql("drop table if exists ratings")
    spark.sql("create table ratings(userId int, itemId int, rating int, timestamp string) row format delimited fields terminated by '\t'")
    spark.sql("load data local inpath 'data/sql/ml-100k/u.data' overwrite into table ratings")
    spark.sql("select * from ratings limit 10").show()

def CreateAndLoadItems(spark):
    spark.sql("drop table if exists items")
    spark.sql("create table items(movieId int, movieTitle string, releaseDate string, videoReleaseDate string, IMDbURL string, unknown int, action int, adventure int, animation int, childrens int, comedy int, crime int, documentary int, drama int, fantasy int, filmNoir int, horror int, musical int, mystery int, romance int, sciFi int, thriller int, war int, western int) row format delimited fields terminated by '|'")
    spark.sql("load data local inpath 'data/sql/ml-100k/u.item' overwrite into table items")
    spark.sql("select movieId, movieTitle from items limit 10").show()

def JoinRatingsAndItems(spark):
    spark.sql("select r.userId, r.itemId, i.movieTitle, r.rating, r.timestamp from ratings r join items i on r.itemId=i.movieId limit 10").show()

if __name__ == "__main__":
    #spark = SparkSession.builder.getOrCreate() #uncomment this for spark-submit, as 'spark' exists in Jupyter.
    CreateAndLoadRatings(spark)
    CreateAndLoadItems(spark)
    JoinRatingsAndItems(spark)
    spark.stop()