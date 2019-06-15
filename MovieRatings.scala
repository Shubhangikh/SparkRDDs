// Operations on RDDs can be run on a Databricks notebook -- coded in Scala
// Movielens dataset downloaded from {https://grouplens.org/datasets/movielens/} and uploaded to Databricks using either wget command or manually uploading
// Reading the files into different RDDs
val movies = spark.read.option("header", true).option("inferSchema", true).csv("/FileStore/tables/movies.csv")
val ratings = spark.read.option("header", true).option("inferSchema", true).csv("/FileStore/tables/ratings.csv")
val tags = spark.read.option("header", true).option("inferSchema", true).csv("/FileStore/tables/tags.csv")

// Movie with the highest count if ratings
import org.apache.spark.sql.functions._
ratings.groupBy("movieId").count().orderBy(desc("count"))

// Movie with the lowest count of ratings
ratings.groupBy("movieId").count().orderBy("count")

// Average rating for each movie
val averageRatings = ratings.groupBy("movieId").avg("rating")
averageRatings.show()

// Movies with the highest average rating
ratings.groupBy("movieId").avg("rating").toDF("movieId", "avgRating").orderBy(desc("avgRating"))

// Movies with the lowest average rating
ratings.groupBy("movieId").avg("rating").toDF("movieId", "avgRating").orderBy("avgRating")

// Join the movies and ratings table and give the names of the top 10 movies with the highest ratings
val avgRatings = ratings.groupBy("movieId").avg("rating").toDF("movieId", "avgRating").orderBy("avgRating")
avgRatings.join(movies, Seq("movieId")).orderBy(desc("avgRating")).show(10)

// Join the movies and tags tables, and output the names of all the movies with the tag "mathematics"
val mathMovies = tags.filter($"tag".contains("mathematics"))
mathMovies.join(movies, mathMovies.col("movieId") === movies.col("movieId"))

// Average rating of movies that contain the tag "artificial intelligence"
val aiMovies = tags.filter($"tag".contains("artificial intelligence"))
val joined = aiMovies.join(ratings, Seq("movieId"))
joined.groupBy("movieId").avg("rating").show()

// Average rating of movies that have "Crime" as the genre
val crimeMovies = movies.filter($"genres".contains("Crime"))
crimeMovies.join(ratings, Seq("movieId")).agg(avg("rating")).show()

// The most popular tag
tags.groupBy("tag").count.orderBy(desc("count")).show(10)
