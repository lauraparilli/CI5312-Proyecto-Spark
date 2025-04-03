from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

# Crear sesión de Spark
spark = SparkSession.builder.appName("AvgRatingByGenre").getOrCreate()

# Cargar movies.csv
movies_df = spark.read.csv("/home/hadoop/hadoop_local/movies_data/letterboxd/movies.csv", 
                           header=True, 
                           inferSchema=True)

# Seleccionar columnas necesarias y filtrar por duración >= 60 min
filtered_movies_df = movies_df.select("id", "minute", "rating").filter(col("minute") >= 60)

# Cargar genres.csv
genres_df = spark.read.csv("/home/hadoop/hadoop_local/movies_data/letterboxd/genres.csv", 
                           header=True, 
                           inferSchema=True)

# JOIN entre películas filtradas y géneros
joined_df = filtered_movies_df.join(genres_df, on="id")

# Agrupar por género y calcular el promedio de rating
avg_rating_df = joined_df.groupBy("genre").agg(avg("rating").alias("avg_rating"))

# Ordenar alfabéticamente
result_df = avg_rating_df.orderBy("genre")

# Mostrar resultados
result_df.show()

# Guardar resultados
result_df.write.csv("/home/hadoop/pyspark/letterboxd_avg_rating", header=True)
