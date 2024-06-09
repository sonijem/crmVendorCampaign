import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    """
    Fixture for creating a Spark session for testing.
    The session will be shared across all tests in the session scope.
    """
    spark = SparkSession.builder \
        .appName("CRM Campaign Engagement Test") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.executor.memory", "1g") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()
    yield spark
    spark.stop()