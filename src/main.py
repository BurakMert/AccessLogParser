from pyspark.sql import Row
from pyspark.sql.functions import col
from pyspark.sql.session import SparkSession
from src.utils.helpers import *

from pyspark.sql.functions import sum

import glob
import time

def count_null(col_name):
    return sum(col(col_name).isNull().cast('integer')).alias(col_name)


def timer(func):
    def wrapper(*args, **kwargs):
        start = time.time()
        func(*args, **kwargs)

        print('The function ran for', time.time() - start)

    return wrapper


@timer
def main():
    spark = SparkSession.builder.appName("Apache Web Server Access Logs Parser").getOrCreate()
    raw_data_files = glob.glob('access_logs/*.gz')
    base_df = spark.read.text(raw_data_files)
    parsed_df = parse_access_log_to_df(base_df)
    country_enriched_df = get_country_from_ip(parsed_df)
    anonymized_df = anonymize_ip(country_enriched_df)
    output_df = anonymized_df.select("anonymized_ip", "rfc1413", "user", "timestamp", "method", "endpoint", "protocol", "status", "content_size")
    output_df.write.mode('overwrite').parquet('output/')




if __name__ == '__main__':
    main()
