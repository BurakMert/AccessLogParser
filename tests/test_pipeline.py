import pytest
from datetime import datetime
from src.utils.helpers import parse_access_log_to_df, get_country_from_ip, anonymize_ip, parse_log_time
from chispa import assert_df_equality, assert_column_equality
from pyspark.sql import Row
from pyspark.sql.types import *


def test_regex_matching_for_ipv4_ipv6(spark):
    data = [
        Row(value='130.119.171.217 - - [01/Jul/1995:12:30:23 -0400] "GET /ksc.html HTTP/1.0" 200 7074'),
        Row(
            value='2001:888:197d:0:250:fcff:fe23:3879 - - [10/Aug/2003:20:28:01 +0200] "GET /ipv6/ksc.html HTTP/1.1" 200 472'),
    ]
    test_schema = StructType([StructField('value', StringType())])
    test_df_raw = spark.createDataFrame(data, test_schema)
    test_df = parse_access_log_to_df(test_df_raw)

    expected_schema = StructType([StructField('host', StringType()),
                                  StructField('rfc1413', StringType()),
                                  StructField('user', StringType()),
                                  StructField('timestamp', StringType()),
                                  StructField('method', StringType()),
                                  StructField('endpoint', StringType()),
                                  StructField('protocol', StringType()),
                                  StructField('status', IntegerType()),
                                  StructField('content_size', IntegerType())
                                  ])
    expected_data = [
        Row(host='130.119.171.217', rfc1413='-', user='-', timestamp='01/Jul/1995:12:30:23 -0400',
            method='GET', endpoint='/ksc.html', protocol='HTTP/1.0', status=200, content_size=7074),
        Row(host='2001:888:197d:0:250:fcff:fe23:3879', rfc1413='-', user='-', timestamp='10/Aug/2003:20:28:01 +0200',
            method='GET', endpoint='/ipv6/ksc.html', protocol='HTTP/1.1', status=200, content_size=472)
    ]
    expected_df = spark.createDataFrame(expected_data, expected_schema)
    assert_df_equality(test_df, expected_df)


def test_ip_country_match(spark):
    schema = StructType([StructField("host", StringType()),
                         StructField("expected_country", StringType())])
    data = [
        Row(host='130.119.171.217', expected_country="US"),
        Row(host='2001:888:197d:0:250:fcff:fe23:3879', expected_country="NL")
    ]
    df = spark.createDataFrame(data, schema)
    ip_country_added_df = get_country_from_ip(df)
    assert_column_equality(ip_country_added_df, "expected_country", "ip_country")

def test_ip_country_no_match(spark):
    schema = StructType([StructField("host", StringType()),
                         StructField("expected_country", StringType())])
    data = [
        Row(host='127.0.0.1', expected_country="NoMatch"),
        Row(host='random.domain.com', expected_country="NotIP")
    ]
    df = spark.createDataFrame(data, schema)
    ip_country_added_df = get_country_from_ip(df)
    assert_column_equality(ip_country_added_df, "expected_country", "ip_country")

def test_ip_anonymizer(spark):
    schema = StructType([StructField("host", StringType()),
                         StructField("ip_country", StringType()),
                         StructField("expected_anonymized_ip", StringType())])
    data = [Row(host='130.119.171.217', ip_country="US", expected_anonymized_ip="130.119.171.US"),
            Row(host='2001:888:197d:0:250:fcff:fe23:3879', ip_country="NL", expected_anonymized_ip="2001:888:197d:0:250:fcff:fe23:NL"),
            Row(host ='random.domain.com', ip_country = "NotIP", expected_anonymized_ip='random.domain.com'),
            Row(host='127.0.0.1', ip_country="NoMatch", expected_anonymized_ip="127.0.0.NaN")
            ]
    df = spark.createDataFrame(data, schema)
    anonymized_df = anonymize_ip(df)
    assert_column_equality(anonymized_df, "expected_anonymized_ip", "anonymized_ip")


def test_time_parser(spark):
    schema = StructType([StructField("timestamp", StringType()),
                         StructField("expected_timestamp", TimestampType())
                         ])
    data = [Row(timestamp='01/Jul/1995:12:30:23 -0400', expected_time=datetime(1995, 7, 1, 19, 30, 23)),
            Row(timestamp='10/Aug/2003:20:28:01 +0200', expected_time=datetime(2003, 8, 10, 21, 28, 1))
            ]
    df = spark.createDataFrame(data, schema)
    parsed_df = parse_log_time(df)
    parsed_df.show(truncate=False)
    assert_column_equality(parsed_df, "expected_timestamp", "parsed_time")
