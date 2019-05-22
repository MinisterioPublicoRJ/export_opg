from base import spark
from timer import timer
from opg_utils import uuidsha


uuidshaudf = spark.udf.register('uuidsha', uuidsha)


print('Generating People/Company\'s connections: Contributory')
with timer():
    print('Building connections and UUID')
    with timer():
        tabela = spark.sql("""from dadossinapse.pessoa_fisica_opv pessoa
            inner join dadossinapse.pessoa_juridica_opv empresa on
            empresa.num_cpf_responsavel = pessoa.num_cpf
            select 'SOCIO_RESPONSAVEL' label,
                    pessoa.uuid start_node,
                    empresa.uuid end_node""").withColumn('uuid', uuidshaudf())

    print('Persisting Contributory OPE')
    with timer():
        tabela.write.mode('overwrite').saveAsTable(
            'dadossinapse.socio_responsavel_ope')

print('Genrating People/Company\'s connection: Partnership')
with timer():
    print('Building connections and UUID')
    with timer():
        tabela = spark.sql("""from bases.lc_socio sociedade
        inner join bases.pessoa_fisica pessoa on
            pessoa.num_cpf = sociedade.cpf_socio
        inner join bases.lc_cnpj empresa on
            empresa.num_cnpj = sociedade.cnpj
        select 'SOCIO' label,
        pessoa.uuid start_node,
        empresa.uuid end_node,
        sociedade.dt_inicio,
        sociedade.dt_fim,
        sociedade.tipo_socio""").withColumn('uuid', uuidshaudf())

    print('Persisting Partnership OPE')
    with timer():
        tabela.write.mode('overwrite').saveAsTable('dadossinapse.socio_ope')

print('Generating Company/Company\'s connection: Partnership')
with timer():
    print('Building connections and UUID')
    with timer():
        tabela = spark.sql("""from bases.lc_socio sociedade
        inner join bases.lc_cnpj empresa1 on
            empresa1.num_cnpj = sociedade.cnpj_socio
        inner join bases.lc_cnpj empresa2 on
            empresa2.num_cnpj = sociedade.cnpj
        select 'SOCIO' label,
                empresa1.uuid start_node,
                empresa1.uuid end_node,
                sociedade.dt_inicio,
                sociedade.dt_fim,
                sociedade.tipo_socio""").withColumn('uuid', uuidshaudf())

    print('Persisting Partnership OPE')
    with timer():
        tabela.write.mode('append').saveAsTable('dadossinapse.socio_ope')
