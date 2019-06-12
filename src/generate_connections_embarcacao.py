from pyspark.sql.functions import lit

from base import spark, BASES, DADOSSINAPSE
from opg_utils import uuidsha
from timer import timer
from context import Database


uuidshaudf = spark.udf.register('uuidsha', uuidsha)


print('Generating watercraft connections')
with timer(), Database(BASES):
    print('Reading tables')
    with timer():
        embarcacao = spark.table('lc_embarcacao')
        pessoa = spark.table('pessoa_fisica')
        empresa = spark.table('lc_cnpj')

        # Merge persons with watercrafts
        pessoa_embarcacao = pessoa.filter('num_cpf is not null').\
            withColumnRenamed('uuid', 'start_node').\
            join(embarcacao, pessoa.num_cpf == embarcacao.cpf_cnpj).\
            select(['start_node', 'uuid']).\
            withColumnRenamed('uuid', 'end_node').\
            withColumn('label', lit('PROPRIETARIO').cast('string')).\
            withColumn('uuid', uuidshaudf())

        empresa_embarcacao = empresa.filter('num_cnpj is not null').\
            withColumnRenamed('uuid', 'start_node').\
            join(embarcacao, empresa.num_cnpj == embarcacao.cpf_cnpj).\
            select(['start_node', 'uuid']).\
            withColumnRenamed('uuid', 'end_node').\
            withColumn('label', lit('PROPRIETARIO').cast('string')).\
            withColumn('uuid', uuidshaudf())

with timer(), Database(DADOSSINAPSE):
        pessoa_embarcacao.write.mode("overwrite").saveAsTable(
            "pessoa_embarcacao_ope")
        empresa_embarcacao.write.mode("overwrite").saveAsTable(
            "empresa_embarcacao_ope")
