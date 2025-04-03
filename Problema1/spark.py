from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, expr

# Inicializar Spark
spark = SparkSession.builder \
    .appName("Problema 1") \
    .master("local[*]") \
    .getOrCreate()

# Crear un DataFrame para tener ratings de letterboxd
dfMovies_letterboxd = spark.read\
.option("header", True)\
.option("inferSchema", True)\
.option("quote",'"')\
.csv("letterboxd/movies.csv")\
.select("id","name","rating", "date")\
.filter("id is not null AND rating is not null AND name is not null AND date is not null")\
.withColumnRenamed("date", "year")

dfMovies_letterboxd = dfMovies_letterboxd.filter("year rlike '^[0-9]{4}$'") # Evitar años con formato incorrecto

# Crear un DataFrame para tener peliculas de imdb
dfMovies_imdb = spark.read\
.option("header", True)\
.option("inferSchema", True)\
.option("delimiter", "\t")\
.csv("imdb/title.basics.tsv")\
.filter("titleType == 'movie'")\
.selectExpr("tconst as id", "primaryTitle as name", "startYear as year")\
.filter("id != '\\N' AND name != '\\N' AND year is not null") # \N pasa a ser nulo al convertirse a int

# Crear un DataFrame para ratings de imdb
dfRatings_imdb = spark.read\
.option("header", True)\
.option("inferSchema", True)\
.option("delimiter", "\t")\
.csv("imdb/title.ratings.tsv")\
.selectExpr("tconst as id","averageRating as rating", "numVotes")\
.filter("rating is not null AND numVotes is not null")

# Seleccionar ratings altos
dfMovies_letterboxd = dfMovies_letterboxd.filter("rating >= 4")

dfHighRatings_imdb = dfRatings_imdb.filter("rating >= 8 and numVotes >= 2100")
dfMovies_imdb = dfMovies_imdb.join(dfHighRatings_imdb, "id", "inner")

# Agrupar por a;o y contar
dfMovies_letterboxd_count = dfMovies_letterboxd.groupBy("year").count()\
                            .withColumnRenamed("count", "count_letterboxd")
dfMovies_imdb_count = dfMovies_imdb.groupBy("year").count().withColumnRenamed("count", "count_imdb")

# Juntar dataframes y sacar 10 ultimos a;os
dfMovies_count = dfMovies_letterboxd_count.join(dfMovies_imdb_count, "year", "outer").na.fill(0)\
                 .orderBy(desc("year")).limit(10)  

# Mostrar el DataFrame
dfMovies_count.show()

# Detener la sesión de Spark
spark.stop()
