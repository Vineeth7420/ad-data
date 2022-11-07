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

    company_df = spark.read \
        .option('header', 'true') \
        .option('mode', 'DROPMALFORMED') \
        .option('delimiter', ',') \
        .option('inferSchema', 'true') \
        .csv('s3a://' + app_conf['s3_conf']['s3_bucket'] + '/All-Live-Shopify-Sites.csv') \
        .toDF('Domain', 'Location on Site', 'Tech Spend USD', 'Sales Revenue USD', 'Social', 'Employees', 'Company', 'Vertical', 'Tranco', 'Page Rank', 'Majestic', 'Umbrella', 'Telephones', 'Emails', 'Twitter', 'Facebook', 'LinkedIn', 'Google', 'Pinterest', 'GitHub', 'Instagram', 'Vk', 'Vimeo', 'Youtube', 'TikTok', 'People', 'City', 'State', 'Zip', 'Country', 'First Detected', 'Last Found', 'First Indexed', 'Last Indexed', 'Exclusion', 'Compliance')

    print('Breaking the File into Smaller files')

    pr_df = company_df\
        .orderBy('Page Rank')

    pr_df \
        .repartition(100) \
        .write \
        .mode('append') \
        .option('header', 'true') \
        .option('delimiter', ',') \
        .csv('s3a://' + app_conf['s3_conf']['s3_bucket'] + '/final_ad_data')

# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4" com/pg/source_data_loading.py

