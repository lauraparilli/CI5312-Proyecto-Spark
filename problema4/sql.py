from pyspark.sql import SparkSession

# 1. Inicialización de SparkSession
spark = SparkSession.builder \
    .appName("IMDB Analysis SQL") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

spark.conf.set("spark.sql.caseSensitive", "true")

# 2. Carga de datos y registro de vistas temporales
def load_and_register_data():
    movies = spark.read.csv(
        "/home/hadoop/imdb_data/csv/movies.csv",
        header=True,
        inferSchema=True
    )
    movies.createOrReplaceTempView("movies")
    
    genres = spark.read.csv(
        "/home/hadoop/imdb_data/csv/genres.csv",
        header=True,
        inferSchema=True
    )
    genres.createOrReplaceTempView("genres")
    
    actors = spark.read.csv(
        "/home/hadoop/imdb_data/csv/actors.csv",
        header=True,
        inferSchema=True
    )
    actors.createOrReplaceTempView("actors")
    
    title_basics = spark.read.csv(
        "/home/hadoop/imdb_data/tsv/title.basics.tsv",
        sep="\t",
        header=True,
        inferSchema=True
    )
    title_basics.createOrReplaceTempView("title_basics")
    
    title_ratings = spark.read.csv(
        "/home/hadoop/imdb_data/tsv/title.ratings.tsv",
        sep="\t",
        header=True,
        inferSchema=True
    )
    title_ratings.createOrReplaceTempView("title_ratings")
    
    title_principals = spark.read.csv(
        "/home/hadoop/imdb_data/tsv/title.principals.tsv",
        sep="\t",
        header=True,
        inferSchema=True
    )
    title_principals.createOrReplaceTempView("title_principals")
    
    name_basics = spark.read.csv(
        "/home/hadoop/imdb_data/tsv/name.basics.tsv",
        sep="\t",
        header=True,
        inferSchema=True
    )
    name_basics.createOrReplaceTempView("name_basics")

# 3. Ejecución de la consulta SQL
def execute_analysis():
    query = """
    WITH high_rated_movies AS (
        SELECT 
            id AS movie_id,
            name AS movie_name,
            date AS release_year,
            rating
        FROM movies
        WHERE rating > 4.0
    ),
    
    movies_genres AS (
        SELECT 
            h.movie_id,
            h.movie_name,
            h.release_year,
            g.genre AS movie_genre
        FROM high_rated_movies h
        JOIN genres g ON h.movie_id = g.id
    ),
    
    movies_details AS (
        SELECT 
            h.movie_id,
            h.movie_name,
            h.release_year,
            t.tconst,
            t.primaryTitle
        FROM high_rated_movies h
        JOIN title_basics t ON h.movie_name = t.primaryTitle 
                           AND h.release_year = t.startYear
    ),
    
    movies_ratings AS (
        SELECT 
            md.*,
            tr.averageRating,
            tr.numVotes
        FROM movies_details md
        JOIN title_ratings tr ON md.tconst = tr.tconst
    ),
    
    movies_principals AS (
        SELECT 
            mr.*,
            tp.nconst,
            tp.category
        FROM movies_ratings mr
        JOIN title_principals tp ON mr.tconst = tp.tconst
    ),
    
    principals_names AS (
        SELECT 
            mp.*,
            nb.primaryName AS actor_name
        FROM movies_principals mp
        JOIN name_basics nb ON mp.nconst = nb.nconst
    ),
    
    final_join AS (
        SELECT 
            pn.movie_id,
            pn.actor_name,
            pn.averageRating,
            pn.numVotes,
            mg.movie_genre AS genre
        FROM principals_names pn
        JOIN movies_genres mg ON pn.movie_id = mg.movie_id
    )
    
    SELECT 
        genre,
        actor_name,
        COUNT(*) AS movie_count,
        ROUND(AVG(averageRating), 2) AS avg_rating,
        SUM(numVotes) AS total_votes,
        ROUND(SUM(numVotes) * AVG(averageRating), 2) AS influence_score
    FROM final_join
    GROUP BY genre, actor_name
    ORDER BY influence_score DESC
    """
    return spark.sql(query)

# 4. Almacenamiento de resultados
def save_results(df):
    (df.write
        .mode("overwrite")
        .option("header", "true")
        .option("delimiter", "|")
        .option("encoding", "UTF-8")
        .option("quote", '"')
        .option("escape", '"')
        .csv("/home/hadoop/imdb_data/output/actor_influence_sql")
    )

# 5. Ejecución del pipeline completo
if __name__ == "__main__":
    try:
        load_and_register_data()
        results_df = execute_analysis()
        
        print("\n" + "="*50)
        print("Vista previa de resultados (Top 10):")
        results_df.show(10, truncate=False)
        
        save_results(results_df)
        print("\n" + "="*50)
        print("Resultados almacenados exitosamente en:")
        print("/home/hadoop/imdb_data/output/actor_influence_sql")
        
        results_df.createOrReplaceTempView("final_results")
        
    except Exception as e:
        print(f"\nError durante la ejecución: {str(e)}")
    finally:
        spark.stop()
        print("\nProceso completado. Sesión de Spark cerrada.")
