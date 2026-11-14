from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Test-Iceberg-S3")
    .getOrCreate()
)

# 1. Création de la base de données dans le catalogue Iceberg
spark.sql("CREATE DATABASE IF NOT EXISTS lakehouse.bronze")

# 2. Création d'une table au format Apache Iceberg
spark.sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.bronze.events (
        event_id INT,
        event_type STRING,
        source STRING
    ) USING iceberg
""")

# 3. Insertion de données de test
spark.sql("""
    INSERT INTO lakehouse.bronze.events VALUES
        (1, 'pipeline_started', 'spark-iceberg'),
        (2, 's3_write_ok', 'lakehouse-minio')
""")

# 4. Lecture et vérification
print("=== CONTENU DE LA TABLE ICEBERG ===")
spark.sql("SELECT * FROM lakehouse.bronze.events ORDER BY event_id").show(truncate=False)

spark.stop()