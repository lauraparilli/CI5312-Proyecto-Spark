from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, desc, rank, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.window import Window

# Crear sesión Spark con configuración para manejar comas en campos
spark = SparkSession.builder \
    .appName("MovieRatingAnalysis") \
    .config("spark.sql.caseSensitive", "true") \
    .getOrCreate()

# Esquema definido
schema = StructType([
    StructField("id", StringType(), True),
    StructField("country", StringType(), True),
    StructField("title", StringType(), True),
    StructField("year", FloatType(), True),
    StructField("rating", FloatType(), True),
    StructField("name", StringType(), True),
    StructField("role", StringType(), True),
    StructField("theme", StringType(), True),
    StructField("genre", StringType(), True),
    StructField("date", StringType(), True),
    StructField("type", StringType(), True),
    StructField("studio", StringType(), True),
    StructField("language", StringType(), True),
])

# Cargar datos con configuración para manejar comas en campos
df = spark.read \
    .option("header", "true") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .option("multiLine", "true") \
    .schema(schema) \
    .csv("combined_all_data.csv")

# Verificar los datos cargados
print("Muestra de datos cargados:")
df.show(5, truncate=False)

# Limpieza de datos: convertir año a entero y filtrar valores nulos
df = df.withColumn("year", col("year").cast(IntegerType())) \
       .filter(col("rating").isNotNull() & col("year").isNotNull())

def analyze_with_dataframes(df):
    # 1. Calcular la valoración promedio por año y género
    yearly_genre_ratings = df.groupBy("year", "genre") \
        .agg(avg("rating").alias("avg_rating"), 
             count("*").alias("movie_count")) \
        .orderBy("year", desc("avg_rating"))
    
    # 2. Identificar las películas mejor valoradas cada año (top 5 por año)
    window_spec = Window.partitionBy("year").orderBy(desc("rating"))
    
    top_movies_per_year = df.withColumn("rank", rank().over(window_spec)) \
        .filter(col("rank") <= 5) \
        .select("year", "title", "genre", "rating", "rank") \
        .orderBy("year", "rank")
    
    # 3. Evolución de la valoración promedio por década
    decade_avg_ratings = df.withColumn("decade", (col("year") / 10).cast("int") * 10) \
        .groupBy("decade") \
        .agg(avg("rating").alias("avg_rating_decade")) \
        .orderBy("decade")
    
    # 4. Tendencias por género (con manejo de valores nulos)
    genre_trends = df.filter(col("genre").isNotNull()).groupBy("genre") \
        .agg(avg(when(col("year") < 2000, col("rating"))).alias("avg_pre_2000"),
             avg(when(col("year") >= 2000, col("rating"))).alias("avg_post_2000")) \
        .withColumn("rating_change", col("avg_post_2000") - col("avg_pre_2000")) \
        .filter(col("avg_pre_2000").isNotNull() & col("avg_post_2000").isNotNull()) \
        .orderBy(desc("rating_change"))
    
    return {
        "yearly_genre_ratings": yearly_genre_ratings,
        "top_movies_per_year": top_movies_per_year,
        "decade_avg_ratings": decade_avg_ratings,
        "genre_trends": genre_trends
    }

# Crear vista temporal para consultas SQL
df.createOrReplaceTempView("movies")

def analyze_with_sql(spark):
    # 1. Valoración promedio por año y género
    yearly_genre_ratings_sql = spark.sql("""
        SELECT year, genre, 
               AVG(rating) as avg_rating, 
               COUNT(*) as movie_count
        FROM movies
        GROUP BY year, genre
        ORDER BY year, avg_rating DESC
    """)
    
    # 2. Top 5 películas por año
    top_movies_per_year_sql = spark.sql("""
        SELECT year, title, genre, rating, rank
        FROM (
            SELECT year, title, genre, rating,
                   RANK() OVER (PARTITION BY year ORDER BY rating DESC) as rank
            FROM movies
        ) ranked
        WHERE rank <= 5
        ORDER BY year, rank
    """)
    
    # 3. Valoración promedio por década
    decade_avg_ratings_sql = spark.sql("""
        SELECT FLOOR(year / 10) * 10 as decade,
               AVG(rating) as avg_rating_decade
        FROM movies
        GROUP BY FLOOR(year / 10) * 10
        ORDER BY decade
    """)
    
    # 4. Tendencias por género
    genre_trends_sql = spark.sql("""
        SELECT genre,
               AVG(CASE WHEN year < 2000 THEN rating END) as avg_pre_2000,
               AVG(CASE WHEN year >= 2000 THEN rating END) as avg_post_2000,
               AVG(CASE WHEN year >= 2000 THEN rating END) - 
               AVG(CASE WHEN year < 2000 THEN rating END) as rating_change
        FROM movies
        WHERE genre IS NOT NULL
        GROUP BY genre
        HAVING avg_pre_2000 IS NOT NULL AND avg_post_2000 IS NOT NULL
        ORDER BY rating_change DESC
    """)
    
    return {
        "yearly_genre_ratings_sql": yearly_genre_ratings_sql,
        "top_movies_per_year_sql": top_movies_per_year_sql,
        "decade_avg_ratings_sql": decade_avg_ratings_sql,
        "genre_trends_sql": genre_trends_sql
    }

# Ejecutar análisis
df_results = analyze_with_dataframes(df)
sql_results = analyze_with_sql(spark)

# Mostrar resultados
print("\n--- Top 5 películas por año \n\nDataFrames:")
df_results["top_movies_per_year"].show(20, truncate=False)
print("\nSQL:")
sql_results["top_movies_per_year_sql"].show(truncate=False)

print("\n--- Tendencias de género por año \n\nDataFrames:")
df_results["yearly_genre_ratings"].show(20,truncate=False)
print("\nSQL:")
sql_results["yearly_genre_ratings_sql"].show(truncate=False)

print("\n--- Promedio de calificación por década\n\nDataFrame:")
df_results["decade_avg_ratings"].show(truncate=False)
print("\nSQL:")
sql_results["decade_avg_ratings_sql"].show(truncate=False)

print("\n--- Tendencias de género (análisis pre/post 2000):\n\nDataframes")
df_results["genre_trends"].show(truncate=False)
print("\nSQL:")
sql_results["genre_trends_sql"].show(truncate=False)
