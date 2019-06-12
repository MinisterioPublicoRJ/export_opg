from base import spark, BASES, DADOSSINAPSE
from opg_utils import uuidsha
from timer import timer
from context import Database

spark.udf.register('uuidsha', uuidsha)

print('Generating father connections')
with timer(), Database(BASES):
    spark.sql("""analyze table pessoa_fisica compute statistics""")
    filhotes = spark.sql("""select
        uuid idpessoa,
        nome_pai,
        data_nascimento
        from pessoa_fisica
        where
        data_nascimento > cast('1800-01-01' as timestamp)
        and data_nascimento < cast('2019-01-01' as timestamp)
        and nome_pai IS NOT NULL and nome_pai != ''""")
    filhotes.registerTempTable("filhotes")
    papais = spark.sql("""select
        uuid idpai,
        nome,
        data_nascimento + interval 13 years reprodutivo_de,
        data_nascimento + interval 50 years reprodutivo_ate
        from pessoa_fisica
        where
        ind_sexo = 'M'
        and data_nascimento > cast('1800-01-01' as timestamp)
        and data_nascimento < cast('2019-01-01' as timestamp)
        and nome IS NOT NULL and nome != ''""")
    papais.registerTempTable("papais")
    tabela = spark.sql("""select idpessoa, idpai
        from filhotes
        inner join papais on
        papais.nome = filhotes.nome_pai
        and (
            filhotes.data_nascimento
            between
                papais.reprodutivo_de
                and
                papais.reprodutivo_ate
        )""")
    tabela.registerTempTable("tabela")
    grupo = spark.sql("""select idpessoa
        from tabela
        group by idpessoa
        having count(idpessoa) <=5""")
    grupo.registerTempTable("grupo")
    resultado = spark.sql(
        """select
                idpessoa start_node,
                idpai end_node,
                uuidsha() uuid,
                'PAI' parentesco,
                'FILHO' label
            from tabela
            where idpessoa in (select idpessoa from grupo)
        """)
with timer(), Database(DADOSSINAPSE):
    resultado.write.mode("overwrite").saveAsTable(
        "pessoa_pai_ope")
