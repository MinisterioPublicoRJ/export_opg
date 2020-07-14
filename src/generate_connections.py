from base import spark, BASES, DADOSSINAPSE
from opg_utils import uuidsha
from context import Database


uuidshaudf = spark.udf.register('uuidsha', uuidsha)


print('Generating People/Company\'s connections: Contributory')
print('Building connections and UUID')
with Database(DADOSSINAPSE):
    tabela = spark.sql("""from pessoa_fisica_opv pessoa
        inner join pessoa_juridica_opv empresa on
        empresa.num_cpf_responsavel = pessoa.num_cpf
        select 'SOCIO_RESPONSAVEL' label,
                pessoa.uuid start_node,
                empresa.uuid end_node""").withColumn('uuid', uuidshaudf())

print('Persisting Contributory OPE')
with Database(DADOSSINAPSE):
    tabela.write.mode('overwrite').saveAsTable(
        'socio_responsavel_ope')

print('Genrating People/Company\'s connection: Partnership')
print('Building connections and UUID')
with Database(BASES):
    tabela = spark.sql("""from lc_socio sociedade
    inner join pessoa_fisica pessoa on
        pessoa.num_cpf = sociedade.cpf_socio
    inner join lc_cnpj empresa on
        empresa.num_cnpj = sociedade.cnpj
    select 'SOCIO' label,
    pessoa.uuid start_node,
    empresa.uuid end_node,
    sociedade.dt_inicio,
    sociedade.dt_fim,
    sociedade.tipo_socio""").withColumn('uuid', uuidshaudf())

print('Persisting Partnership OPE')
with Database(DADOSSINAPSE):
    tabela.write.mode('overwrite').saveAsTable('socio_ope')

print('Generating Company/Company\'s connection: Partnership')
print('Building connections and UUID')
with Database(BASES):
    tabela = spark.sql("""from lc_socio sociedade
    inner join lc_cnpj empresa1 on
        empresa1.num_cnpj = sociedade.cnpj_socio
    inner join lc_cnpj empresa2 on
        empresa2.num_cnpj = sociedade.cnpj
    select 'SOCIO' label,
            empresa1.uuid start_node,
            empresa1.uuid end_node,
            sociedade.dt_inicio,
            sociedade.dt_fim,
            sociedade.tipo_socio""").withColumn('uuid', uuidshaudf())

print('Persisting Partnership OPE')
with Database(DADOSSINAPSE):
    tabela.write.mode('append').saveAsTable('socio_ope')