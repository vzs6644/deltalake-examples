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

    delta_merge_path = "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/delta_merge_delta"

    step = "create"
    if step == 'create':
        delta_merge_df = sc.parallelize([
            ("Brazil",  2011, 22.029),
            ("India", 2006, 24.73)
        ]).toDF(["country", "year", "temperature"])
        delta_merge_df.show()
        print("Writing data..")
        delta_merge_df \
            .write \
            .mode("overwrite") \
            .format("delta") \
            .save(delta_merge_path)
        print("Write completed!")

    elif step == 'merge':
        delta_merge_df = DeltaTable.forPath(spark, delta_merge_path)
        print("Creating another data,")
        updates_df = sc.parallelize([
          ("Australia", 2019, 24.0),
          ("India", 2006, 25.05),
          ("India", 2010, 27.05)
        ]).toDF(["country", "year", "temperature"])
        updates_df.show()

        print("Merging them both for matching country and year,")
        delta_merge_df.alias("delta_merge") \
            .merge(updates_df.alias("updates"),
                   "delta_merge.country = updates.country and delta_merge.year = updates.year") \
            .whenMatchedUpdate(set = {"temperature": "updates.temperature"}) \
            .whenNotMatchedInsert(values = {"country": "updates.country", "year": "updates.year", "temperature": "updates.temperature"}) \
            .execute()

        delta_merge_df.toDF().show()


# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4,io.delta:delta-core_2.11:0.6.0" com/dsm/delta/merge_demo.py
