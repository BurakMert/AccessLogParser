# AccessLogParser
This repository is used for anonymizing IP addresses in web server logs. The process assumes the following log format to work:
LogFormat "%h %l %u %t \"%r\" %>s %O

You need Apache Spark and pyspark installed for this repository to work. You can install the pip libraries by using requirements.txt as follows:
* pip install -r requirements.txt

In order to install Apache Spark you can use the following guide:

* for Macos : https://medium.com/macoclock/how-to-install-apache-pyspark-on-macbook-pro-4a9249f0d823

* for Ubuntu and Debian : https://computingforgeeks.com/how-to-install-apache-spark-on-ubuntu-debian/

The parser looks logs under access_logs directory and expects the logs in .gz format. I used this format as input format because usually you find large log files compressed in .gz files. As an input I have also added Access Logs provided by NASA for July and August. 

Parser first finds valid IP addresses in host column, and then looks up the country for the given IP address. System is tested against both IPv4 and IPv6. You can run those tests by using the following command:

* python -m pytest tests/

When anonymizing IP addresses my approach was splitting the IP address by delimeter(. for IPv4 and : for IPv6) and replacing last part with country information. An example of IP anonymization is below:
130.119.171.217 -> 130.119.171.US

I am using maxminddb and GeoLite-Country.mmdb database for country lookup. When there is no match for a given IP, NaN is returned from country enrichment transformation. Also when the host is not an IP address but a domain name I assumed that no further anaoymization is needed. When we encounter an ip address without a country match anonymization process adds NaN instead of last part for the IP address. You can find an example below:
127.0.0.1 -> 127.0.0.NaN

After anaoymization process log files are written back to outputs directory. 

Some key points to consider:

* Log files are usually large files for web servers with some traffic. Hence doing this parsing in memory is not an option since you can easily have log files larger than your memory. Thats why I used Spark for that purpose. Rather than doing the process in memory, spark runs the operations on disk and fetching data to memory in chunks when needed. That eliminates memory problems when dealing with large log files. Also when building a data pipeline Spark becomes very usefull with its ecosystem. You can submit those Spark jobs on your local machine, on a Spark Cluster running on cloud or managed services that runs Spark jobs like Glue on AWS.
* For output format I have chosen parquet. Parquet is a columnar format which can compress data data extremely well. Also when you are building a data pipeline and especially most of the work you do is revolved in manupulating whole column parquet can save you both from space and processing time. I thought this anonymizing process could be a first step of a datapipeline, basically preparing data for further analysis. In this scenario one can read the parquet files from the output directory and build another pipe for processing, like counting logs by day/hour, analyzing incoming traffic by country etc. All those analysis could benefit from storing the data in a compressed columnar format like parquet.
* When anonymizing IP address I have several concerns about data validity, ingtegrity and variation of the data. For host column you can get domain names, IPv4 addresses and Ipv6 addresess as well. In order to deal with all of them I have designed an anonymization transform which basically understands the format of the host(domain, IPv4 or Ipv6 are supported). If its a domain no further transformation is needed, for any IP address parser tries to find country and replaces last chunk of IP with country value. With a valid IP if system cant find a country match it anonymizes IP by replacing last chunck with "NaN"
