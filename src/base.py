import pyspark
from timer import timer


with timer():
    print('Creating Spark Session')
    spark = pyspark.sql.session.SparkSession\
        .builder\
        .appName("export_opg")\
        .enableHiveSupport()\
        .getOrCreate()

    sc = spark.sparkContext

    spark.sql("SET hive.mapred.supports.subdirectories=true")
    spark.sql("SET mapreduce.input.fileinputformat.input.dir.recursive=true")

    sc.setCheckpointDir('hdfs:///user/felipeferreira/temp')
