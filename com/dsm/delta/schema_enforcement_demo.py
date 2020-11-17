from pyspark.sql import SparkSession
from delta.tables import *
import os.path
import yaml

if __name__ == '__main__':
    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("Read Files") \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.4,io.delta:delta-core_2.11:0.6.0') \
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
        .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.hadoop.fs.s3a.access.key", app_secret["s3_conf"]["access_key"]) \
        .config("spark.hadoop.fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"]) \
        .getOrCreate()

    sc = spark.sparkContext
    spark.sparkContext.setLogLevel('ERROR')

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    # Setup spark to use s3
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])

    delta_table_path = "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/schema_enforcement_delta"

    step = "append"
    if step == "overwrite":
        data = sc.parallelize([
            ("Brazil",  2011, 22.029),
            ("India", 2006, 24.73)
          ]) \
          .toDF(["country", "year", "temperature"])
        data.printSchema()
        data.show()
        print("Writing data..")
        data \
            .coalesce(1) \
            .write \
            .format("delta") \
            .mode("overwrite") \
            .save(delta_table_path)
        print("Write completed!")

        print("Reading data,")
        DeltaTable.forPath(spark, delta_table_path).toDF().show()

    elif step == "append":
        new_data = sc.parallelize([
            ("Australia", 2019.0, 30.0)
            ]) \
            .toDF(["country", "year", "temperature"])
        new_data.printSchema()
        new_data.show()
        print("Writing data..")
        new_data \
            .write \
            .format("delta") \
            .mode("append") \
            .save(delta_table_path)

# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4,io.delta:delta-core_2.11:0.6.0" com/dsm/delta/schema_enforcement_demo.py
