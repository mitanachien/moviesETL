# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import (
    col,
    current_timestamp,
    when,
    explode,
    from_unixtime,
    lag,
    lead,
    lit,
    abs,
    mean,
    stddev,
    max,
)
from typing import List
from pyspark.sql.session import SparkSession
from pyspark.sql.window import Window

# COMMAND ----------

def read_batch_raw(moviePath: str) -> DataFrame:
  df = spark.read.option("multiline","true").json(moviePath + "*.json")
  return df.select(explode(df.movie).alias("movie"))

# COMMAND ----------

def transform_raw(raw: DataFrame) -> DataFrame:
    return raw.select(
        lit("antra.movie.json.files").alias("datasource"),
        current_timestamp().alias("ingesttime"),
        "movie",
        lit("new").alias("status"),
        current_timestamp().cast("date").alias("p_ingestdate")
    )

# COMMAND ----------

def batch_writer(
    dataframe: DataFrame,
    partition_column: str,
    exclude_columns: List = [],
    mode: str = "append",
) -> DataFrame:
    return (
        dataframe.drop(
            *exclude_columns
        )  # This uses Python argument unpacking (https://docs.python.org/3/tutorial/controlflow.html#unpacking-argument-lists)
        .write.format("delta")
        .mode(mode)
        .partitionBy(partition_column)
    )


# COMMAND ----------

def create_table(table: str, path: str) -> bool:
  spark.sql("CREATE TABLE IF NOT EXISTS " + table + " USING DELTA LOCATION " + '"' + path + '"')
  return True

# COMMAND ----------

def read_batch_bronze(spark: SparkSession) -> DataFrame:
  df = (spark.read
  .table("movie_bronze")
  .filter("status = 'new'")
  )
  return df

# COMMAND ----------

def read_loaded_bronze(spark: SparkSession) -> DataFrame:
  df = (spark.read
  .table("movie_bronze")
  .filter("status = 'loaded'")
  )
  return df

# COMMAND ----------

def bronze_drop_duplicates(newBronze: DataFrame, loadedBronze: DataFrame):
  matched = bronzeDF.select("movie").intersect(bronzeLoadedDF.select("movie"))
  joinedDF = bronzeDF.join(matched, on = bronzeDF.movie == matched.movie, how = 'inner').select(bronzeDF["*"])
  finalDF = bronzeDF.exceptAll(joinedDF)
  return finalDF

# COMMAND ----------

def transform_bronze(bronze: DataFrame, quarantine: bool = False) -> DataFrame:
    silver_all = bronze.select("movie", "movie.*")
    genre_all = silver_all.select(
            explode(col("genres")).alias("genres")
        )
    
    if not quarantine:
        silver_genres = genre_all.select(
            col("genres.id").alias("genres_id"), 
            col("genres.name").alias("genres_name")
        )
        silver_genres = silver_genres.distinct().filter("genres_name <> ''").orderBy(silver_genres.genres_id.asc())
        
        silver_movies = silver_all.select(
            "movie",
            col("BackdropUrl").alias("Moviephoto"),
            col("Budget").cast("long"),
            col("CreatedBy"),
            col("CreatedDate").cast("timestamp"),
            col("Id"),
            col("ImdbUrl").alias("IMDB"),
            col("OriginalLanguage").alias("Language"),
            col("Overview"),
            col("PosterUrl").alias("Poster"),
            col("Price"),
            col("ReleaseDate").cast("timestamp"),
            col("Revenue"),
            col("RunTime").cast("int"),
            col("Tagline").alias("Tag"),
            col("Title"),
            col("TmdbUrl").alias("TMDB"),
            col("UpdatedBy"),
            col("UpdatedDate").cast("timestamp"),
            silver_all.genres.id.alias("genres"),
            col("CreatedDate").cast("date").alias("p_eventdate")
        )
        silver_movies = silver_movies.distinct()

    return silver_genres, silver_movies


# COMMAND ----------

def generate_clean_and_quarantine_dataframes(
    dataframe: DataFrame,
) -> (DataFrame, DataFrame):
  silverCleanDF = dataframe.filter("RunTime >= 0 AND Budget >= 1000000")
  silverQuarantineDF = dataframe.filter("RunTime < 0 OR Budget < 1000000")
  return silverCleanDF, silverQuarantineDF

# COMMAND ----------

def lookup_writer(
    dataframe: DataFrame,
    mode: str = "overwrite"
) -> DataFrame:
    return (
        dataframe
        .write.format("delta")
        .mode(mode)
    )

# COMMAND ----------

def genre_drop_duplicates(newGenre: DataFrame, loadedGenre: DataFrame):
  combined = newGenre.union(loadedGenre)
  combined = combined.dropDuplicates(["genres_id"])
  return combined

# COMMAND ----------

def read_batch_delta(deltaPath: str) -> DataFrame:
    return spark.read.format("delta").load(deltaPath)

# COMMAND ----------

def update_bronze_table_status(
    spark: SparkSession, bronzeTablePath: str, dataframe: DataFrame, status: str
) -> bool:

    bronzeTable = DeltaTable.forPath(spark, bronzePath)
    dataframeAugmented = dataframe.withColumn("status", lit(status))

    update_match = "bronze.movie = dataframe.movie"
    update = {"status": "dataframe.status"}

    (
        bronzeTable.alias("bronze")
        .merge(dataframeAugmented.alias("dataframe"), update_match)
        .whenMatchedUpdate(set=update)
        .execute()
    )

    return True


# COMMAND ----------

def repair_quarantined_records(
    spark: SparkSession, bronzeTable: str) -> DataFrame:
    bronzeQuarantinedDF = (spark.read.table(bronzeTable).filter("status = 'quarantined'"))
    bronzeQuarTransGenreDF, bronzeQuarTransMovieDF = transform_bronze(bronzeQuarantinedDF)
    
    fixBudgetMovieDF = (bronzeQuarTransMovieDF.withColumn("Budget", when(bronzeQuarTransMovieDF.Budget < 1000000, lit(1000000)).otherwise(bronzeQuarTransMovieDF.Budget))
                                  .withColumn("Budget", col("Budget").cast("long")))
    fixRuntimeMovieDF = (fixBudgetMovieDF.withColumn("RunTime", abs(col("RunTime"))))
    
    return bronzeQuarTransGenreDF, fixRuntimeMovieDF


# COMMAND ----------

def raw_to_bronze(moviePath: str) -> bool:
  # read json files and add information
  rawDF = read_batch_raw(moviePath)
  transformedRawDF = transform_raw(rawDF)
  
  # Write to bronze table
  rawToBronzeWriter = batch_writer(
      dataframe=transformedRawDF, partition_column="p_ingestdate"
  )
  rawToBronzeWriter.save(bronzePath)
  create_table("movie_bronze", bronzePath)
  
  # Purge raw file path
  # dbutils.fs.rm(moviePath, recurse=True)
  
  return True

# COMMAND ----------

def bronze_to_silver(spark: SparkSession) -> bool:
  # Read new bronze data and loaded bronze data
  bronzeDF = read_batch_bronze(spark)
  bronzeLoadedDF = read_loaded_bronze(spark)

  # Drop duplicates data to ensure only one record shows up in the silver table
  finalBronzeDF = bronze_drop_duplicates(bronzeDF, bronzeLoadedDF)
  
  if finalBronzeDF.count() != 0:
    # Transform bronze data
    (transformedBronzeGenre,  transformedBronzeMovie) = transform_bronze(finalBronzeDF)

    # Clean data and separate them to clean and quarantine
    (silverCleanDF, silverQuarantineDF) = generate_clean_and_quarantine_dataframes(
        transformedBronzeMovie
    )

    # Write movie_silver table
    bronzeToSilverWriterMovie = batch_writer(
        dataframe = silverCleanDF, partition_column = "p_eventdate", exclude_columns = ["movie"]
    )
    bronzeToSilverWriterMovie.save(movieSilverPath)
    create_table("movie_silver", movieSilverPath)

    # Write grnre_silver table
    loadedGenreDF = read_batch_delta(genreSilverPath)
    finalGenre = genre_drop_duplicates(transformedBronzeGenre, loadedGenreDF)
    bronzeToSilverWriterGenre = lookup_writer(
        dataframe = finalGenre
    )
    bronzeToSilverWriterGenre.save(genreSilverPath)
    create_table("genre_silver", genreSilverPath)

    # Update bronze status
    update_bronze_table_status(spark, bronzePath, silverCleanDF, "loaded")
    update_bronze_table_status(spark, bronzePath, silverQuarantineDF, "quarantined")
  
  return True

# COMMAND ----------

def silver_update(spark: SparkSession) -> bool:
  # Repair quarantined records
  silverCleanedGenreDF, silverCleanedMovieDF = repair_quarantined_records(spark, bronzeTable = "movie_bronze")

  # Write to movie_silver table
  bronzeToSilverWriterMovie = batch_writer(
      dataframe = silverCleanedMovieDF, partition_column = "p_eventdate", exclude_columns = ["movie"]
  )
  bronzeToSilverWriterMovie.save(movieSilverPath)

  # Write to grnre_silver table
  loadedGenreDF = read_batch_delta(genreSilverPath)
  finalGenre = genre_drop_duplicates(silverCleanedGenreDF, loadedGenreDF)
  bronzeToSilverWriterGenre = lookup_writer(
      dataframe = finalGenre
  )
  bronzeToSilverWriterGenre.save(genreSilverPath)

  # Update bronze status
  update_bronze_table_status(spark, bronzePath, silverCleanedMovieDF, "loaded")
  
  return True