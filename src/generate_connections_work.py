from base import spark
from timer import timer


print('Generating People/Company\'s connections: Work')
with timer():
    print('Building connections and UUID')
    with timer():
        tabela = spark.sql("""select
                vinculo.uuid,
                'TRABALHA' label,
                pessoa.uuid start_node,
                empresa.uuid end_node,
                vinculo.dt_inicio,
                vinculo.dt_fim,
                vinculo.vinc_ativo
            from bases.lc_vinculo_trabalhista vinculo
            inner join bases.pessoa_fisica pessoa on
                pessoa.num_cpf = vinculo.cpf
            inner join bases.lc_cnpj empresa on
                empresa.num_cnpj = vinculo.cnpj""")

    print('Persisting Work OPE')
    with timer():
        tabela.write.mode('overwrite').saveAsTable(
            'dadossinapse.vinculo_trabalhista_ope')
