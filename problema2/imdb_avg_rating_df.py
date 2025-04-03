from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode, avg

# Crear sesión de Spark
spark = SparkSession.builder.appName("IMDB_AvgRatingByGenre").getOrCreate()

# Cargar title.basics.tsv
basics_df = spark.read.option("header", False) \
                      .option("delimiter", "\t") \
                      .option("inferSchema", True) \
                      .csv("/home/hadoop/hadoop_local/movies_data/imdb/title.basics.tsv") \
                      .toDF("tconst", "titleType", "primaryTitle", "originalTitle",
                            "isAdult", "startYear", "endYear", "runtimeMinutes", "genres")

# Filtrar películas con duración válida y >= 60 min
basics_valid_df = basics_df.filter(
    (col("runtimeMinutes") != "\\N") &
    (col("runtimeMinutes").cast("int") >= 60)
)

# Cargar title.ratings.tsv
ratings_df = spark.read.option("header", False) \
                       .option("delimiter", "\t") \
                       .option("inferSchema", True) \
                       .csv("/home/hadoop/hadoop_local/movies_data/imdb/title.ratings.tsv") \
                       .toDF("tconst", "averageRating", "numVotes")

# JOIN por tconst
joined_df = basics_valid_df.join(ratings_df, on="tconst")

# Separar géneros múltiples y expandir filas
exploded_df = joined_df.withColumn("genre", explode(split(col("genres"), ","))) \
                       .select("averageRating", "genre")

# Agrupar por género y calcular promedio
avg_rating_df = exploded_df.groupBy("genre").agg(avg("averageRating").alias("avg_rating"))

# Filtrar géneros nulos y cortos
filtered_df = avg_rating_df.filter((col("genre") != "\\N") & (col("genre") != "Short"))

# Ordenar por género
result_df = filtered_df.orderBy("genre")

# Mostrar resultado
print("=== Resultado con DataFrame API ===")
result_df.show()

# Guardar resultado
result_df.write.csv("/home/hadoop/pyspark/imdb_avg_rating_dataframe", header=True)

spark.stop()
