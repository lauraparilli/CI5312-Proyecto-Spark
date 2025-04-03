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

# Registrar los DataFrames como vistas temporales
filtered_movies_df.createOrReplaceTempView("movies")
genres_df.createOrReplaceTempView("genres")

# Escribir y ejecutar consulta SQL
result_sql_df = spark.sql("""
    SELECT g.genre, AVG(m.rating) AS avg_rating
    FROM movies m
    JOIN genres g ON m.id = g.id
    WHERE m.minute >= 60
    GROUP BY g.genre
    ORDER BY g.genre ASC
""")

# Mostrar resultados
result_sql_df.show()

# Guardar resultados
result_sql_df.write.csv("/home/hadoop/pyspark/letterboxd_avg_rating_sql", header=True)
