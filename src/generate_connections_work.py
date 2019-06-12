from base import spark, BASES, DADOSSINAPSE
from timer import timer
from context import Database


print('Generating People/Company\'s connections: Work')
print('Building connections and UUID')
with timer(), Database(BASES):
    tabela = spark.sql("""select
            vinculo.uuid,
            'TRABALHA' label,
            pessoa.uuid start_node,
            empresa.uuid end_node,
            vinculo.dt_inicio,
            vinculo.dt_fim,
            vinculo.vinc_ativo
        from lc_vinculo_trabalhista vinculo
        inner join pessoa_fisica pessoa on
            pessoa.num_cpf = vinculo.cpf
        inner join lc_cnpj empresa on
            empresa.num_cnpj = vinculo.cnpj""")

print('Persisting Work OPE')
with timer(), Database(DADOSSINAPSE):
    tabela.write.mode('overwrite').saveAsTable(
        'vinculo_trabalhista_ope')
