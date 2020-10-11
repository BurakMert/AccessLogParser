import pytest
from pyspark.sql.session import SparkSession


@pytest.fixture(scope='session')
def spark():
    return SparkSession.builder \
      .appName("test") \
        .getOrCreate()
