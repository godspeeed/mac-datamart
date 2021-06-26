import sys as system

def get_redshift_jdbc_url(redshift_config: dict):
    host = redshift_config["redshift_conf"]["host"]
    port = redshift_config["redshift_conf"]["port"]
    database = redshift_config["redshift_conf"]["database"]
    username = redshift_config["redshift_conf"]["username"]
    password = redshift_config["redshift_conf"]["password"]
    return "jdbc:redshift://{}:{}/{}?user={}&password={}".format(host, port, database, username, password)


def get_mysql_jdbc_url(mysql_config: dict):
    host = mysql_config["mysql_conf"]["hostname"]
    port = mysql_config["mysql_conf"]["port"]
    database = mysql_config["mysql_conf"]["database"]
    return "jdbc:mysql://{}:{}/{}?autoReconnect=true&useSSL=false".format(host, port, database)

def write_to_s3(df, path):
    print("Writing Data to PATH :-------", path)
    df.write \
        .mode("append") \
        .partitionBy("ins_dt") \
        .parquet(path)

def filter_out_argument_source_list():
    default_source_list = ["SB", "OL", "CP", "ADDR"]
    input_argv = [x.upper() for x in system.argv]
    argument_source_list = input_argv
    final_list = list(set(default_source_list).intersection(argument_source_list))
    print("** Filtering Out **", final_list)
    return final_list