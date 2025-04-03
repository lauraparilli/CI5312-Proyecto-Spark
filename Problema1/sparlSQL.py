from pyspark.sql import SparkSession

# Inicializar Spark
spark = SparkSession.builder \
    .appName("Consulta SQL en Spark") \
    .master("local[*]") \
    .getOrCreate()

# Cargar los datos en DataFrames
dfMovies_letterboxd = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("quote", '"') \
    .csv("letterboxd/movies.csv") \
    .select("id", "name", "rating", "date") \
    .filter("id is not null AND rating is not null AND name is not null AND date is not null") \
    .withColumnRenamed("date", "year")

dfMovies_imdb = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("delimiter", "\t") \
    .csv("imdb/title.basics.tsv") \
    .filter("titleType == 'movie'") \
    .selectExpr("tconst as id", "primaryTitle as name", "startYear as year") \
    .filter("id != '\\N' AND name != '\\N' AND year is not null")

dfRatings_imdb = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("delimiter", "\t") \
    .csv("imdb/title.ratings.tsv") \
    .selectExpr("tconst as id", "averageRating as rating", "numVotes") \
    .filter("rating is not null AND numVotes is not null")

# Registrar DataFrames como tablas SQL temporales
dfMovies_letterboxd.createOrReplaceTempView("letterboxd_movies_table")
dfMovies_imdb.createOrReplaceTempView("imdb_movies_table")
dfRatings_imdb.createOrReplaceTempView("imdb_ratings_table")

# Consulta SQL en Spark
query = """
WITH letterboxd_movies AS (
    SELECT 
        id, 
        name, 
        rating, 
        CAST(year AS INT) AS year
    FROM letterboxd_movies_table
    WHERE year RLIKE '^[0-9]{4}$' AND rating >= 4
),
imdb_movies AS (
    SELECT 
        id, 
        name, 
        CAST(year AS INT) AS year
    FROM imdb_movies_table
),
imdb_ratings AS (
    SELECT 
        id, 
        rating, 
        numVotes
    FROM imdb_ratings_table
    WHERE rating >= 8 AND numVotes >= 2100
),
filtered_imdb AS (
    SELECT 
        m.id, 
        m.name, 
        m.year
    FROM imdb_movies m
    INNER JOIN imdb_ratings r ON m.id = r.id
),
letterboxd_count AS (
    SELECT 
        year, 
        COUNT(*) AS count_letterboxd
    FROM letterboxd_movies
    GROUP BY year
),
imdb_count AS (
    SELECT 
        year, 
        COUNT(*) AS count_imdb
    FROM filtered_imdb
    GROUP BY year
),
final_result AS (
    SELECT 
        COALESCE(l.year, i.year) AS year, 
        COALESCE(l.count_letterboxd, 0) AS count_letterboxd, 
        COALESCE(i.count_imdb, 0) AS count_imdb
    FROM letterboxd_count l
    FULL OUTER JOIN imdb_count i ON l.year = i.year
)
SELECT * 
FROM final_result
ORDER BY year DESC
LIMIT 10
"""

# Ejecutar la consulta SQL en Spark
df_result = spark.sql(query)
df_result.show()

# Detener Spark
spark.stop()
