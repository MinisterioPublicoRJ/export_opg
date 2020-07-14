# coding=utf-8
from base import spark, BASES
from timer import timer
from context import Database

from pyspark.sql.functions import col, regexp_replace, when
from opg_utils import uuidsha

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import unicodedata

uuidshaudf = spark.udf.register('uuidshaudf', uuidsha)

accent_replacements = [
    (u'á|ã|â|à', 'a'), (u'Á|Ã|Â|À', 'A'),
    (u'é|ê', 'e'), (u'É|Ê', 'E'),
    (u'í', 'i'), (u'Í', 'I'),
    (u'ó|ô|õ', 'o'), (u'Ó|Ô|Õ', 'O'),
    (u'ú|ü', 'u'), (u'Ú|Ű', 'U'),
    (u'ç', 'c'), (u'Ç', 'C')
]


def remove_accents(column):
    r = col(column)
    for a, b in accent_replacements:
        r = regexp_replace(r, a, b)
    return r.alias('remove_accents(' + column + ')')


def remove_accents_n(value):
    text = unicodedata.normalize('NFD', value)
    text = text.encode('ascii', 'ignore')
    text = text.decode("utf-8")
    return text


def check_type(df, (col_name, dtype)):
    list_columns = [
        'no_cidadao', 
        'no_paicidadao', 
        'no_maecidadao', 
        'naturalidade', 
        'documento_origem', 
        'endereco', 
        'bairro', 
        'municipio'
        ]
    if col_name in list_columns:
        remove_accents_udf = udf(remove_accents_n, StringType())
        df = df.withColumn(col_name, remove_accents_udf(col_name))
    return df


lc_cpf_columns = """A.num_cpf AS num_cpf,
A.nome AS nome,
A.nome_mae AS nome_mae,
A.data_nascimento AS data_nascimento,
A.ind_sexo as ind_sexo,
A.num_titulo_eleitor as num_titulo_eleitor,
A.tipo_logradouro as tipo_logradouro,
A.descr_logradouro as descr_logradouro,
A.num_logradouro as num_logradouro,
A.descr_complemento_logradouro as descr_complemento_logradouro,
A.nome_bairro as nome_bairro,
A.num_cep as num_cep,
A.nome_municipio as nome_municipio,
A.sigla_uf as sigla_uf,
A.num_ddd as num_ddd,
A.num_telefone as num_telefone,
A.num_fax as num_fax,
A.se_estrangeiro as se_estrangeiro,
A.nome_pais_nacionalidade as nome_pais_nacionalidade,
A.cod_situacao_cadastral as cod_situacao_cadastral,
A.descr_situacao_cadastral as descr_situacao_cadastral,
A.data_situacao_cadastral as data_situacao_cadastral,
A.data_inscricao as data_inscricao,
A.ano_obito as ano_obito,
A.ano_ultima_entrega_declaracao as ano_ultima_entrega_declaracao"""

rgcivil_columns = """B.nu_rg as num_rg,
lpad(B.nu_cpf,11,'0') as nu_cpf_rg,
UPPER(B.no_cidadao) as nome_rg,
UPPER(B.no_paicidadao) as nome_pai,
UPPER(B.no_maecidadao) as nome_mae_rg,
B.dt_expedicao_carteira as dt_expedicao_rg,
B.dt_nascimento as data_nascimento_rg,
UPPER(B.naturalidade) as naturalidade_rg,
UPPER(B.documento_origem) as documento_origem_rg,
UPPER(B.endereco) as endereco_rg,
UPPER(B.bairro) as nome_bairro_rg,
UPPER(B.municipio) as nome_municipio_rg,
UPPER(B.uf) as sigla_uf_rg,
B.cep as num_cep_rg"""

rgcivil_null_list = []
for x in rgcivil_columns.split('\n'):
    nl = ['NULL']
    nl.extend(x.split(" ")[1:])
    rgcivil_null_list.append(" ".join(nl))
rgcivil_null_list = "\n".join(rgcivil_null_list)

lc_cpf_null_list = []
for x in lc_cpf_columns.split('\n'):
    nl = ['NULL']
    nl.extend(x.split(" ")[1:])
    lc_cpf_null_list.append(" ".join(nl))
lc_cpf_null_list = "\n".join(lc_cpf_null_list)

selected_columns = """
    {},
    {}
""".format(lc_cpf_columns, rgcivil_columns)

print('Generating Pessoa Table')
with Database(BASES):
    print('Joining lc_cpf and detran_regcivil')
    
    # max_dt = spark.sql("""
    #     SELECT nu_rg, MAX(dt_expedicao_carteira) as max_date
    #     FROM detran_regcivil
    #     GROUP BY nu_rg
    # """)
    # max_dt.registerTempTable("max_dt")
    # detran_max_exp_dt = spark.sql("""
    #     SELECT A.* FROM detran_regcivil A
    #     INNER JOIN max_dt
    #     ON A.nu_rg = max_dt.nu_rg AND
    #     (A.dt_expedicao_carteira = max_dt.max_date
    #     OR (A.dt_expedicao_carteira IS NULL AND max_dt.max_date IS NULL))
    # """)

    detran_max_exp_dt = spark.table('detran.vw_regcivil')

    # detran_max_exp_dt = detran_max_exp_dt.withColumn(
    #     'no_cidadao', remove_accents('no_cidadao'))
    # detran_max_exp_dt = detran_max_exp_dt.withColumn(
    #     ''no_paicidadao'', remove_accents('no_paicidadao'))
    # detran_max_exp_dt = detran_max_exp_dt.withColumn(
    #     'no_maecidadao', remove_accents('no_maecidadao'))
    # detran_max_exp_dt = detran_max_exp_dt.withColumn(
    #     'naturalidade', remove_accents('naturalidade'))
    # detran_max_exp_dt = detran_max_exp_dt.withColumn(
    #     'documento_origem', remove_accents('documento_origem'))
    # detran_max_exp_dt = detran_max_exp_dt.withColumn(
    #     'endereco', remove_accents('endereco'))
    # detran_max_exp_dt = detran_max_exp_dt.withColumn(
    #     'bairro', remove_accents('bairro'))
    # detran_max_exp_dt = detran_max_exp_dt.withColumn(
    #     'municipio', remove_accents('municipio'))
    
    detran_max_exp_dt = reduce(check_type, detran_max_exp_dt.dtypes, detran_max_exp_dt)
    detran_max_exp_dt.registerTempTable("detran_max_exp_dt")

    inners = spark.sql("""
        SELECT
        {}, 'CPF' as motivo_juncao
        FROM lc_cpf A
        INNER JOIN detran_max_exp_dt B ON A.num_cpf = lpad(B.nu_cpf, 11, '0')
        UNION ALL
        SELECT
        {}, 'NOME NOME_MAE DT_NASCIMENTO' as motivo_juncao
        FROM lc_cpf A
        INNER JOIN detran_max_exp_dt B ON
            A.nome = UPPER(B.no_cidadao) AND
            A.nome_mae = UPPER(B.no_maecidadao) AND
            A.data_nascimento = B.dt_nascimento
        WHERE B.nu_cpf = ''
        """.format(selected_columns, selected_columns)
    )
    inners.registerTempTable("inners")
    spark.catalog.cacheTable('inners')

    tabela = spark.sql("""
        SELECT
        {},{}, NULL as motivo_juncao
        FROM lc_cpf A
        WHERE NOT EXISTS
            (
                SELECT 1
                FROM inners I
                WHERE I.num_cpf = A.num_cpf
            )
        UNION ALL
        SELECT
        {},{}, NULL as motivo_juncao
        FROM detran_max_exp_dt B
        WHERE NOT EXISTS
            (
                SELECT 1
                FROM inners I
                WHERE I.num_rg = B.nu_rg
            )
        UNION ALL
        SELECT * FROM inners
    """.format(
        lc_cpf_columns,
        rgcivil_null_list,
        lc_cpf_null_list,
        rgcivil_columns)
    ).withColumn(
        'uuid',
        uuidshaudf(
            col('num_rg'),
            col('nu_cpf_rg'),
            col('dt_expedicao_rg'),
            col('nome_rg'),
            col('nome_pai'),
            col('nome_mae_rg'),
            col('naturalidade_rg'),
            col('documento_origem_rg'),
            col('endereco_rg'),
            col('nome_bairro_rg'),
            col('nome_municipio_rg'),
            col('sigla_uf_rg'),
            col('num_cep_rg'),
            col('num_cpf'),
            col('nome'),
            col('data_nascimento').cast('string'),
            col('ind_sexo'),
            col('nome_mae')
        ))
    tabela.registerTempTable('tabela')
    tabela = spark.sql("""
        SELECT tabela.*,
        case when lc_ppe.cpf is not null then 1
        else 0
        end as sensivel
        FROM tabela
        LEFT JOIN staging.lc_ppe ON lc_ppe.cpf = tabela.num_cpf
    """)
    tabela.withColumn("nome", when(col("nome") == "", col("nome_rg")).otherwise(col("nome")))

    tabela.write.mode('overwrite').saveAsTable('pessoa_fisica')
    spark.catalog.clearCache()
