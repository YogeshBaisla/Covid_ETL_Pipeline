from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jproperties import Properties
#creating dataframe from data and cleaning its value

config = Properties()
with open('/home/hadoop/datacodeconfig.properties', 'rb') as config_file:
    config.load(config_file)


def cleanData():
    MAX_MEMORY = "4g"
    spark = SparkSession.builder.config("spark.jars", config.get('SparkJasrs')[0]).appName("CovidProject").config("spark.executor.memory", MAX_MEMORY).config("spark.driver.memory", MAX_MEMORY).getOrCreate()

    sdf = spark.read.format("jdbc").option("url",config.get('MysqlUrl')[0]) \
        .option("driver", config.get('driver')[0]).option("dbtable", config.get('dbtable')[0]) \
        .option("user", config.get('user')[0]).option("password", config.get('password')[0]).load()
    sdf.show(5)
    print(sdf.columns)
    colums = sdf.columns
    needColums = ['active','cured','death','positive','state_name','Repo_Date']
    for i in colums:
        if i in needColums:
            continue
        else:
            sdf = sdf.drop(i)
    sdf = sdf.withColumn("active",sdf.active.cast("integer"))
    sdf = sdf.withColumn("cured",sdf.cured.cast("integer"))
    sdf = sdf.withColumn("death",sdf.death.cast("integer"))
    sdf = sdf.withColumn("positive",sdf.positive.cast("integer"))
    sdf = sdf.withColumn("Repo_Date",sdf.Repo_Date.cast("date"))
    sdf = sdf.withColumn("Years",year(sdf.Repo_Date))
    sdf = sdf.withColumn("Months",month(sdf.Repo_Date))
    sdf.printSchema()
    sdf = sdf.dropna()
    sdf = sdf.select([when(col(c) == "", None).otherwise(
          col(c)).alias(c) for c in sdf.columns])
    sdf = sdf.dropna()
    sdf = sdf.withColumn("state_name", regexp_replace(
          col("state_name"), "[^a-zA-Z_\-]+", ""))
    sdf = sdf.withColumn("state_name", when(sdf.state_name == "Telengana", "Telangana")
                          .when(sdf.state_name == "Karanataka", "Karnataka")
                          .when(sdf.state_name == "HimanchalPradesh", "HimachalPradesh")
                          .when(sdf.state_name == "DadraandNagarHaveli", "DadraandNagarHaveliandDamanandDiu")
                          .when(sdf.state_name == "DamanandDiu", "DadraandNagarHaveliandDamanandDiu")
                          .otherwise(sdf.state_name))
    #to s3 
    spark.conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    spark.conf.set("fs.s3a.access.key",config.get('accesskey')[0])
    spark.conf.set("fs.s3a.secret.key",config.get('secretkey')[0])
    spark.conf.set("fs.s3a.endpoint",config.get('endpoint')[0])
    spark.conf.set("com.amazonaws.services.s3.enableV4", "true")
    #full data
    sdf.write.option("header", "true").mode("overwrite").parquet(config.get('s3path')[0],compression="snappy")

    #partioned data
    sdf.write.option("header", "true").mode("overwrite").partitionBy("state_name","Years","Months").parquet(config.get('s3path2')[0],compression="snappy")
cleanData()