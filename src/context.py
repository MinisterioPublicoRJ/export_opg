from base import spark, READONLYDB


class Database:
    def __init__(self, name):
        self.name = name

    def __enter__(self):
        spark.sql('use %s' % self.name)

    def __exit__(self, *args, **kwargs):
        spark.sql('use %s' % READONLYDB)
