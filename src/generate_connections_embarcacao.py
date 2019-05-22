from pyspark.sql.functions import lit

from base import spark
from opg_utils import uuidsha
from timer import timer


uuidshaudf = spark.udf.register('uuidsha', uuidsha)


print('Generating watercraft connections')
with timer():
    print('Reading tables')
    with timer():
        embarcacao = spark.table('bases.lc_embarcacao')
        pessoa = spark.table('bases.pessoa_fisica')
        empresa = spark.table('bases.lc_cnpj')

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

        pessoa_embarcacao.write.mode("overwrite").saveAsTable(
            "dadossinapse.pessoa_embarcacao_ope")
        empresa_embarcacao.write.mode("overwrite").saveAsTable(
            "dadossinapse.empresa_embarcacao_ope")
