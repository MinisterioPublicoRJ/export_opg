import hashlib
import uuid
import os
import random


def update_uuid(table, spark, destination, uuidfunc, coalesce, partcount=300):
    "Generates UUID and copy table to destination database"
    
    if coalesce:
        spark_table = spark.table(table).coalesce(partcount)
    else:
        spark_table = spark.table(table).repartition(partcount)
    #spark_table = spark.table(table).coalesce(partcount)
    spark_table = spark_table.withColumn(
        'uuid',
        uuidfunc
    )
    spark_table.write\
        .mode('overwrite')\
        .format("parquet")\
        .saveAsTable(destination)

    # teste de economia de memoria
    #spark_table.unpersist()


def cuuid():
    return str(uuid.uuid4().int & (1 << 60)-1)


def limpa(entrada):
    if isinstance(entrada, unicode):
        return entrada.encode('ascii', errors='ignore')
    elif isinstance(entrada, str):
        return entrada.decode(
            'ascii', errors='ignore').encode('ascii') if entrada else ""
    else:
        return ''


random.seed(os.getpid())


def uuidsha(*argv):
    if not argv:
        return hashlib.sha1(str(random.random())).hexdigest()

    _ = limpa
    return hashlib.sha1(reduce(lambda x, y: _(x)+_(y), argv)).hexdigest()
