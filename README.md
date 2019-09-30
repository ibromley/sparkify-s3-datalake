# Sparkify S3 Data Warehouse

An S3 based data warehouse for understanding the listing habits of Sparkify users


## Purpose
This database serves analytical data repository for Sparkify, a startup which building a new music streaming application. They intend to perform analytics on understand the listening habits of their users. Ideally, the want to know which users are playing certain songs and further gather all relevant metadata about the track played. Currently all their data is stored in Amazon S3, and we will first load the data into Spark for transformation and then load the transformed data back into the S3 based datawarehouse.

## Setup

### Requirements

You'll need an [AWS](https://aws.amazon.com/) account with the resources to make ~20k Simple Storage Service Requests ($0.004 per request, more pricing details [here](https://aws.amazon.com/s3/pricing/))

You'll also need this software installed on your system 
* [Python](https://www.python.org/downloads/)
* [Java 8](https://www.oracle.com/technetwork/java/javase/downloads/jre8-downloads-2133155.html)
* [Apache Spark](http://spark.apache.org/downloads.html)
* Optional: [Anaconda](https://www.anaconda.com/distribution/#download-section)

You will need to setup some environment variables before starting PySpark, and you can persist them by adding them to your `~/.bashprofile`. Open it with your favorite text editor and add these lines:

```
export JAVA_HOME=$(/usr/libexec/java_home)
export SPARK_HOME=~/spark-2.3.0-bin-hadoop2.7
export PATH=$SPARK_HOME/bin:$PATH
export PYSPARK_PYTHON=python3
```


### Quick Start

Create a new empty S3 bucket within AWS utilizing either the Quick Launch wizard, [AWS CLI](https://docs.aws.amazon.com/cli/index.html), or the various AWS SDKs (e.g. [boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html) for python).

Update the credentials and configuration details in `dl.cfg`. Change the S3 destination bucket location to the path you just created.

Peform the extraction, transformation and loading of the database tables by running the `etl.py` script.

```
$ python etl.py
```

## Project Structure

```
sparkify-redshift-dwh/
 ├── README.md              Documentation of the project
 ├── dwh.cfg                Configuration file for setting up S3 sources/destinations & AWS credentials
 └── etl.py                 Python script to load data from S3 and load it back into the datawarehouse
```