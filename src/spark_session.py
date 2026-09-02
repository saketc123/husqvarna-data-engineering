from pyspark.sql import SparkSession


def get_spark_session() -> SparkSession:
    """Create and return the Spark session for the project."""
    return (
        SparkSession.builder
        .appName("HusqvarnaDataEngineering")
        .master("local[*]")
        .getOrCreate()
    )