from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum, count, desc

# Inicializar SparkSession
spark = SparkSession.builder \
    .appName("IMDB Analysis") \
    .getOrCreate()

# Configuración para manejar cabeceras en CSV/TSV
spark.conf.set("spark.sql.caseSensitive", "true")

## 1. Cargar los datos (modificar rutas según sea necesario)

# Cargar movies.csv (Letterboxd)
movies = spark.read.csv(
    "/home/hadoop/imdb_data/csv/movies.csv",
    header=True,
    inferSchema=True
).select(
    col("id").alias("movie_id"),
    col("name").alias("movie_name"),
    col("date").alias("release_year"),
    "tagline",
    "description",
    col("minute").alias("runtime_minutes"),
    "rating"
)

# Filtrar películas con rating alto
high_rated_movies = movies.filter(col("rating") > 4.0)

# Cargar géneros CSV
genres = spark.read.csv(
    "/home/hadoop/imdb_data/csv/genres.csv",
    header=True,
    inferSchema=True
).select(
    col("id").alias("genre_movie_id"),
    col("genre").alias("movie_genre")
)

# Cargar actores CSV
actors = spark.read.csv(
    "/home/hadoop/imdb_data/csv/actors.csv",
    header=True,
    inferSchema=True
).select(
    col("id").alias("actor_movie_id"),
    col("name").alias("local_actor_name"),
    "role"
)

## 2. Cargar datasets IMDB (TSV)

# Cargar title.basics.tsv
title_basics = spark.read.csv(
    "/home/hadoop/imdb_data/tsv/title.basics.tsv",
    sep="\t",
    header=True,
    inferSchema=True
).select(
    col("tconst").alias("basics_tconst"),
    "titleType",
    "primaryTitle",
    "originalTitle",
    "isAdult",
    "startYear",
    "endYear",
    "runtimeMinutes",
    "genres"
)

# Cargar title.ratings.tsv
title_ratings = spark.read.csv(
    "/home/hadoop/imdb_data/tsv/title.ratings.tsv",
    sep="\t",
    header=True,
    inferSchema=True
).select(
    col("tconst").alias("ratings_tconst"),
    "averageRating",
    "numVotes"
)

# Cargar title.principals.tsv
title_principals = spark.read.csv(
    "/home/hadoop/imdb_data/tsv/title.principals.tsv",
    sep="\t",
    header=True,
    inferSchema=True
).select(
    col("tconst").alias("principals_tconst"),
    "ordering",
    col("nconst").alias("principals_nconst"),
    "category",
    "job",
    "characters"
)

# Cargar name.basics.tsv
name_basics = spark.read.csv(
    "/home/hadoop/imdb_data/tsv/name.basics.tsv",
    sep="\t",
    header=True,
    inferSchema=True
).select(
    col("nconst").alias("name_nconst"),
    "primaryName",
    "birthYear",
    "deathYear",
    "primaryProfession",
    "knownForTitles"
)

## 3. Procesamiento de datos

# JOIN películas con géneros
movies_genres = high_rated_movies.join(
    genres,
    high_rated_movies.movie_id == genres.genre_movie_id,
    "inner"
)

# JOIN por nombre y año en vez de ID
movies_details = high_rated_movies.join(
    title_basics,
    (high_rated_movies.movie_name == title_basics.primaryTitle) & 
    (high_rated_movies.release_year == title_basics.startYear),
    "inner"
)

# JOIN películas con ratings usando tconst
movies_ratings = movies_details.join(
    title_ratings,
    movies_details.basics_tconst == title_ratings.ratings_tconst,
    "inner"
)

# JOIN con actores principales IMDB
movies_principals = movies_ratings.join(
    title_principals,
    movies_ratings.ratings_tconst == title_principals.principals_tconst,
    "inner"
)

# JOIN actores principales con nombres IMDB
principals_names = movies_principals.join(
    name_basics,
    movies_principals.principals_nconst == name_basics.name_nconst,
    "inner"
)

# Seleccionar columnas necesarias
actors_detailed = principals_names.select(
    col("movie_id"),
    col("averageRating"),
    col("numVotes"),
    col("primaryName").alias("actor_name")
)

# JOIN final con géneros usando movie_id
final_join = actors_detailed.join(
    movies_genres,
    actors_detailed.movie_id == movies_genres.movie_id,
    "inner"
)

# Seleccionar columnas para el resultado final
final_projection = final_join.select(
    col("movie_genre").alias("genre"),
    col("actor_name"),
    col("averageRating"),
    col("numVotes")
)

## 4. cálculos adicionales

# Agrupar por (género, actor) y calcular métricas
actor_influence = final_projection.groupBy("genre", "actor_name") \
    .agg(
        count("*").alias("movie_count"),
        avg("averageRating").alias("avg_rating"),
        sum("numVotes").alias("total_votes"),
        (sum("numVotes") * avg("averageRating")).alias("influence_score")
    )

# Ordenar resultados por influence_score descendente
sorted_actor_influence = actor_influence.orderBy(desc("influence_score"))

## 5. Guardar resultados

# Guardar como archivos CSV particionados
sorted_actor_influence.write \
    .mode("overwrite") \
    .csv("/home/hadoop/imdb_data/output/actor_influence", header=True)

# Mostrar algunos resultados para verificación
print("Resultados finales (Top 10 actores más influyentes por género):")
sorted_actor_influence.show(10, truncate=False)

# Detener la sesión de Spark
spark.stop()
