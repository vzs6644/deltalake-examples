from pyspark.sql import SparkSession
import os.path
import yaml

if __name__ == '__main__':
    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("Read Files") \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.4') \
        .getOrCreate()

    sc = spark.sparkContext
    spark.sparkContext.setLogLevel('ERROR')

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    # Setup spark to use s3
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])

    df_path = "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/schema_enforcement"

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
            .write.format("parquet") \
            .mode("overwrite") \
            .save(df_path)
        print("Write completed!")

        print("Reading data,")
        spark.read.parquet(df_path).show()

    elif step == "append":
        new_data = sc.parallelize([
            ("Australia", 2019.0, 30.0)
            ]) \
            .toDF(["country", "year", "temperature"])
        new_data.printSchema()
        new_data.show()
        print("Writing data..")
        new_data \
            .coalesce(1) \
            .write.format("parquet") \
            .mode("append") \
            .save(df_path)
        print("Write completed!")

        print("Reading data,")
        spark.read.parquet(df_path).show()

# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4" com/dsm/df/limits/schema_enforcement_demo.py