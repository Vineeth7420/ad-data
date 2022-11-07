from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import yaml
import os.path

if __name__ == '__main__':
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
        .appName("Ad-data") \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set('f3.s3a.access.key', app_secret['s3_conf']['access_key'])
    hadoop_conf.set('f3.s3a.secret.key', app_secret['s3_conf']['secret_access_key'])

    finance_df = spark.read \
        .option('header', 'true') \
        .option('mode', 'DROPMALFORMED') \
        .option('delimiter', ',') \
        .option('inferSchema', 'true') \
        .csv('s3a://' + app_conf['s3_conf']['s3_bucket'] + '/All-Live-Shopify-Sites.csv') \
        .toDF()


# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4" ad-data/com/pg/source_data_loading.py

