from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode

# Crear sesión de Spark
spark = SparkSession.builder.appName("IMDB_AvgRatingByGenre_SQL").getOrCreate()

# Cargar y nombrar columnas del archivo basics
basics_df = spark.read.option("header", False) \
                      .option("delimiter", "\t") \
                      .option("inferSchema", True) \
                      .csv("/home/hadoop/hadoop_local/movies_data/imdb/title.basics.tsv") \
                      .toDF("tconst", "titleType", "primaryTitle", "originalTitle",
                            "isAdult", "startYear", "endYear", "runtimeMinutes", "genres")

# Filtrar duración válida y >= 60 min
basics_valid_df = basics_df.filter(
    (basics_df["runtimeMinutes"] != "\\N") &
    (basics_df["runtimeMinutes"].cast("int") >= 60)
)

# Cargar y nombrar columnas del archivo ratings
ratings_df = spark.read.option("header", False) \
                       .option("delimiter", "\t") \
                       .option("inferSchema", True) \
                       .csv("/home/hadoop/hadoop_local/movies_data/imdb/title.ratings.tsv") \
                       .toDF("tconst", "averageRating", "numVotes")

# JOIN por tconst
joined_df = basics_valid_df.join(ratings_df, on="tconst")

# Explode de géneros
joined_exploded_df = joined_df.withColumn("genre", explode(split(joined_df["genres"], ",")))

# Filtrar \N y Short
joined_exploded_df = joined_exploded_df.filter(
    (joined_exploded_df.genre.isNotNull()) &
    (joined_exploded_df.genre != "\\N") &
    (joined_exploded_df.genre != "Short")
)

# Crear vista para SQL
joined_exploded_df.createOrReplaceTempView("genre_data")

# Consulta SQL
result_sql_df = spark.sql("""
    SELECT genre, AVG(averageRating) AS avg_rating
    FROM genre_data
    GROUP BY genre
    ORDER BY genre ASC
""")

# Mostrar resultados
print("=== Resultado con SQL ===")
result_sql_df.show()

# Guardar resultados
result_sql_df.write.csv("/home/hadoop/pyspark/imdb_avg_rating_sql", header=True)

spark.stop()
