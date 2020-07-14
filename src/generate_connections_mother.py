from base import spark, BASES, DADOSSINAPSE
from opg_utils import uuidsha
from context import Database

spark.udf.register('uuidsha', uuidsha)

print('Generating mother connections')
with Database(BASES):
    spark.sql("""analyze table pessoa_fisica compute statistics""")
    filhotes = spark.sql("""select
        uuid idpessoa,
        nome_mae,
        data_nascimento
        from pessoa_fisica
        where
        data_nascimento > cast('1800-01-01' as timestamp)
        and data_nascimento < cast('2020-01-01' as timestamp)
        and nome_mae IS NOT NULL and nome_mae != ''""")
    filhotes.registerTempTable("filhotes")
    mamaes = spark.sql("""select
        uuid idmae,
        nome,
        data_nascimento + interval 13 years reprodutivo_de,
        data_nascimento + interval 50 years reprodutivo_ate
        from pessoa_fisica
        where
        ind_sexo = 'F'
        and data_nascimento > cast('1800-01-01' as timestamp)
        and data_nascimento < cast('2020-01-01' as timestamp)
        and nome IS NOT NULL and nome != ''""")
    mamaes.registerTempTable("mamaes")
    tabela = spark.sql("""select idpessoa, idmae
        from filhotes
        inner join mamaes on
        mamaes.nome = filhotes.nome_mae
        and (
            filhotes.data_nascimento
            between
                mamaes.reprodutivo_de
                and
                mamaes.reprodutivo_ate
        )""")
    tabela.registerTempTable("tabela")
    grupo = spark.sql("""select idpessoa
        from tabela
        group by idpessoa
        having count(idpessoa) <=5""")
    grupo.registerTempTable("grupo")
    resultado = spark.sql("""
        select
            idpessoa start_node,
            idmae end_node,
            uuidsha() uuid,
            'MAE' parentesco,
            'FILHO' label
        from tabela
        where idpessoa in (select idpessoa from grupo)""")

with Database(DADOSSINAPSE):
    resultado.write.mode("overwrite").saveAsTable(
        "pessoa_mae_ope")
