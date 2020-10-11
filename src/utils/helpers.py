import maxminddb

from pyspark.sql.functions import regexp_extract
from src.utils.regex_patterns import *
from pyspark.sql.types import *
from pyspark.sql.functions import udf, col
from datetime import datetime
from dateutil.parser import parse

db = maxminddb.open_database('src/resources/GeoLite2-Country.mmdb', maxminddb.reader.MODE_MEMORY)

def _find_country_from_ip(ip):
    try:
        country_code = "NoMatch"
        match = db.get(ip)
        if match is not None:
            if match.get("country", None):
                country_code = match["country"]["iso_code"]
            elif match.get("registered_country", None):
                country_code = match["registered_country"]["iso_code"]
            else:
                country_code = match["continent"]["code"]
        return country_code
    except (ValueError, KeyError):
        return "NaN"


def _validate_and_find_country_from(ip):
    try:
        match = ip_pattern_compiled.match(ip)
        if match is not None:
            return _find_country_from_ip(match.group(1))
        else:
            return "NotIP"
    except ValueError:
        return "NaN"

_ip_validate_and_get_country = udf(_validate_and_find_country_from, StringType())

def _anonymize_ip(ip, country):
    try:
        if country != 'NotIP':
            delimiter = '.' if len(ip.split('.')) > 1 else ':'
            ip_addr_splitted = ip.split(delimiter)
            if country != 'NoMatch':
                anonymized_ip = ip_addr_splitted[:-1] + [country]
            else:
                anonymized_ip = ip_addr_splitted[:-1] + ["NaN"]
            return delimiter.join(anonymized_ip)
        else:
            return ip
    except Exception as e:
        print(e)
        return "NaN"
_anonymizer = udf(_anonymize_ip, StringType())

def _parse_timestamp(time_column):
    try:
        parsed_time = parse(time_column[:11] + " " + time_column[12:])
        return parsed_time
    except Exception as e:
        return datetime(1970, 1, 1, 0, 0, tzinfo=None)

_parse_log_time = udf(_parse_timestamp, TimestampType())

def parse_log_time(df):
    return df.withColumn("parsed_time", _parse_log_time('timestamp'))

def anonymize_ip(df):
    """
    This method anonymizes ip with country and returns a new df with column anonymized_ip
    Method only anonymizes valid IPv4 and IPv6 type hosts, returns "Nan" for the rest If
    there is a domain name in host column we are assuming that information is anonymized enough.
    :param df:
    :return:
    """
    return df.withColumn("anonymized_ip", _anonymizer('host', 'ip_country'))

def parse_access_log_to_df(base_df):
    """
    This method parses incoming df into access log df according to "%h %l %u %t \"%r\" %>s %O format
    :param base_df:
    :return:
    """
    parsed_df = base_df.select(regexp_extract('value', ip_rfc_user, 1).alias('host'),
                               regexp_extract('value', ip_rfc_user, 2).alias('rfc1413'),
                               regexp_extract('value', ip_rfc_user, 3).alias('user'),
                               regexp_extract('value', ts_pattern, 1).alias('timestamp'),
                               regexp_extract('value', method_uri_protocol_pattern, 1).alias('method'),
                               regexp_extract('value', method_uri_protocol_pattern, 2).alias('endpoint'),
                               regexp_extract('value', method_uri_protocol_pattern, 3).alias('protocol'),
                               regexp_extract('value', status_pattern, 1).cast('integer').alias('status'),
                               regexp_extract('value', content_size_pattern, 1).cast('integer').alias('content_size'))
    return parsed_df

def get_country_from_ip(df):
    """
    This method returns anonymizes all ips from the incoming df and returns anonimyzed df
    :param df:
    :return: anonymized_df
    """
    return df.withColumn('ip_country', _ip_validate_and_get_country('host'))