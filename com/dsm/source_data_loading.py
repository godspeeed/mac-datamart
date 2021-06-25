from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import yaml
import utils.utilities as ut
import os.path
import sys as system
import boto3

if __name__ == '__main__':

    # Create the SparkSession
    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("Read ingestion enterprise applications") \
        .master('local[*]') \
        .config("spark.mongodb.input.uri", app_secret["mongodb_config"]["uri"]) \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    # Setup spark to use s3
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])

    print("******** Testing Data", ut.filter_out_argument_source_list())
    arg_filter = ut.filter_out_argument_source_list()
    src_list = app_conf["source_list"]

    if len(arg_filter) != 0:
        src_list = arg_filter
    else:
        print("else statement print........")

    print("src_List Is :- ", src_list)
    print("src_List Is arg_filter :- ", arg_filter)
    print("src_List Is arg_filter length :- ", len(arg_filter))

    for src in src_list:

        output_path = "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/" + app_conf["s3_conf"]["staging_dir"] + "/" + src
        src_conf = app_conf[src]
        if src == "OL":
            # MARK: SFTP
            print("Read loyalty data from SFTP folder and write it ot S3 bucket")
            ol_txn_df = spark.read \
                .format("com.springml.spark.sftp") \
                .option("host", app_secret["sftp_conf"]["hostname"]) \
                .option("port", app_secret["sftp_conf"]["port"]) \
                .option("username", app_secret["sftp_conf"]["username"]) \
                .option("pem", os.path.abspath(current_dir + "/../../" + app_secret["sftp_conf"]["pem"])) \
                .option("fileType", "csv") \
                .option("delimiter", "|") \
                .load(src_conf["sftp_conf"]["directory"] + "/receipts_delta_GBR_14_10_2017.csv")
            ol_txn_df = ol_txn_df.withColumn("ins_dt", current_date())
            ol_txn_df.show(5, False)
            ut.write_to_s3(ol_txn_df, output_path)

        elif src == "SB":
            # MARK: MySQL
            print("\nRead loyalty data from MySql db and write it ot S3 bucket")
            jdbc_params = {"url": ut.get_mysql_jdbc_url(app_secret),
                           "lowerBound": "1",
                           "upperBound": "100",
                           "dbtable": src_conf["mysql_conf"]["dbtable"],
                           "numPartitions": "2",
                           "partitionColumn": src_conf["mysql_conf"]["partition_column"],
                           "user": app_secret["mysql_conf"]["username"],
                           "password": app_secret["mysql_conf"]["password"]
                           }
            # print(jdbcParams)
            # use the ** operator/un-packer to treat a python dictionary as **kwargs
            print("\nReading ta from MySql DB using SparkSession.ead.format(),")
            txn_DF = spark \
                .read.format("jdbc") \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .options(**jdbc_params) \
                .load()
            txn_DF = txn_DF.withColumn("ins_dt", current_date())
            txn_DF.show()
            ut.write_to_s3(txn_DF, output_path)

        elif src == "CP":
            # MARK: S3
            print("\nRead loyalty data from S3 and write it ot S3 bucket")
            cp_df = spark.read \
                .option("mode", "DROPMALFORMED") \
                .option("header", "false") \
                .option("delimiter", "|") \
                .option("inferSchema", "true") \
                .csv("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/KC_Extract_1_20171009.csv")
            cp_df = cp_df.withColumn("ins_dt", current_date())
            ut.write_to_s3(cp_df, output_path)

        elif src == "ADDR":
            # MARK: MongoDB
            print("\nRead loyalty data from MongoDB and write it ot S3 bucket")
            cust_aadr = spark \
                .read \
                .format("com.mongodb.spark.sql.DefaultSource") \
                .option("database", app_conf["mongodb_config"]["database"]) \
                .option("collection", app_conf["mongodb_config"]["collection"]) \
                .load()
            cust_aadr = cust_aadr.withColumn("ins_dt", current_date())
            cust_aadr.show()
            ut.write_to_s3(cust_aadr, output_path)

        else:
            print("******* Please enter any one of the following: \nSB\nOL\nCP\nADDR\n\n******* For Multiple enter comma separated e.g: SB,DL \n\n Default execution:- SB ")

# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4,org.mongodb.spark:mongo-spark-connector_2.11:2.4.1,mysql:mysql-connector-java:8.0.15,com.springml:spark-sftp_2.11:1.1.1" com/dsm/source_data_loading.py
