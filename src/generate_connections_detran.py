from pyspark.sql.functions import expr, lit
from pyspark.sql.types import StringType

from base import spark
from opg_utils import uuidsha
from timer import timer

uuidshaudf = spark.udf.register('uuidsha', uuidsha)

print('Generating ticket connections')
with timer():
    print('Reading tables')
    with timer():
        pessoa = spark.table('bases.pessoa_fisica')
        multa = spark.table('bases.detran_multa')
        veiculo = spark.table('bases.detran_veiculo')
        empresa = spark.table('bases.lc_cnpj')

        # Removing left padding zeros
        multa = multa.withColumn("cpf", expr(
            "substring(ident2, 4, length(ident2))"))
        veiculo = veiculo.withColumn("cpf", expr(
            "substring(cpfcgc, 4, length(cpfcgc))"))
        veiculo.registerTempTable("veiculo_cpf")
        multa.registerTempTable("multa_cpf")

        pessoas_com_carro = spark.sql(
            """select *
            from bases.pessoa_fisica
            where num_cpf in (select cpf from veiculo_cpf)"""
        )

        # Merge persons with tickets
        pessoa_multa = pessoa.filter('num_cpf is not null').\
            withColumnRenamed('uuid', 'start_node').\
            join(multa, pessoas_com_carro.num_cpf == multa.cpf).\
            select(['start_node', 'uuid']).\
            withColumnRenamed('uuid', 'end_node').\
            withColumn('label', lit('AUTUADO').cast(StringType())).\
            withColumn('uuid', uuidshaudf())

        veiculo_multa = veiculo.filter('cpfcgc is not null').\
            withColumnRenamed('uuid', 'start_node').\
            join(multa, veiculo.placa == multa.pl_vei_inf).\
            select(['start_node', 'uuid']).\
            withColumnRenamed('uuid', 'end_node').\
            withColumn('label', lit('AUTUADO').cast(StringType())).\
            withColumn('uuid', uuidshaudf())

        pessoa_veiculo = pessoa.filter('num_cpf is not null').\
            withColumnRenamed('uuid', 'start_node').\
            join(veiculo, pessoa.num_cpf == veiculo.cpf).\
            select(['start_node', 'uuid']).\
            withColumnRenamed('uuid', 'end_node').\
            withColumn('label', lit('PROPRIETARIO').cast(StringType())).\
            withColumn('uuid', uuidshaudf())

        empresa_veiculo = empresa.filter('num_cnpj is not null').\
            withColumnRenamed('uuid', 'start_node').\
            join(veiculo, empresa.num_cnpj == veiculo.cpfcgc).\
            select(['start_node', 'uuid']).\
            withColumnRenamed('uuid', 'end_node').\
            withColumn('label', lit('PROPRIETARIO').cast(StringType())).\
            withColumn('uuid', uuidshaudf())

        empresa_multa = empresa.filter('num_cnpj is not null').\
            withColumnRenamed('uuid', 'start_node').\
            join(multa, empresa.num_cnpj == multa.ident2).\
            select(['start_node', 'uuid']).\
            withColumnRenamed('uuid', 'end_node').\
            withColumn('label', lit('AUTUADO').cast(StringType())).\
            withColumn('uuid', uuidshaudf())

        # Persist tables
        pessoa_multa.write.mode("overwrite").saveAsTable(
            "dadossinapse.pessoa_multa_ope")
        veiculo_multa.write.mode("overwrite").saveAsTable(
            "dadossinapse.veiculo_multa_ope")
        pessoa_veiculo.write.mode("overwrite").saveAsTable(
            "dadossinapse.pessoa_veiculo_ope")
        empresa_veiculo.write.mode("overwrite").saveAsTable(
            "dadossinapse.empresa_veiculo_ope")
        empresa_multa.write.mode("overwrite").saveAsTable(
            "dadossinapse.empresa_multa_ope")
