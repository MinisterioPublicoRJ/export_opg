from base import spark, BASES, DADOSSINAPSE
from opg_utils import uuidsha
from timer import timer
from pyspark.sql.functions import lit, lower
from pyspark.sql.types import StringType
from context import Database

uuidshaudf = spark.udf.register('uuidshaudf', uuidsha)

with Database(BASES):
    spark.sql('analyze table orgaos compute statistics')
    spark.sql('analyze table lc_cnpj compute statistics')
    spark.sql('analyze table documentos compute statistics')
    spark.sql('analyze table personagem compute statistics')
    spark.sql('analyze table pessoa_fisica compute statistics')
    spark.sql('analyze table exadata.mcpr_pessoa_fisica compute statistics')

    orgaos = spark.table('orgaos')
    empresas = spark.table('lc_cnpj')
    documentos = spark.table('documentos')
    personagem = spark.table('personagem')
    det_pessoa_fisica = spark.table('pessoa_fisica')
    mgp_pessoa_fisica = spark.table('exadata.mcpr_pessoa_fisica')

    pessoa_fisica_cpf = det_pessoa_fisica.join(
        mgp_pessoa_fisica,
        det_pessoa_fisica.num_cpf == mgp_pessoa_fisica.PESF_CPF
    )

    pessoa_fisica_rg = det_pessoa_fisica.join(
        mgp_pessoa_fisica,
        det_pessoa_fisica.num_rg == mgp_pessoa_fisica.PESF_NR_RG
    )
    
    pessoa_fisica_nome_mae = det_pessoa_fisica.join(
        mgp_pessoa_fisica,
        (det_pessoa_fisica.nome == mgp_pessoa_fisica.PESF_NM_PESSOA_FISICA)
        & (det_pessoa_fisica.nome_mae == mgp_pessoa_fisica.PESF_NM_MAE)
    )

    pessoa_fisica_nome_mae_rg = det_pessoa_fisica.join(
        mgp_pessoa_fisica,
        (det_pessoa_fisica.nome == mgp_pessoa_fisica.PESF_NM_PESSOA_FISICA)
        & (det_pessoa_fisica.nome_mae_rg == mgp_pessoa_fisica.PESF_NM_MAE)
    )

    pessoa_fisica_nome_nasc = det_pessoa_fisica.join(
        mgp_pessoa_fisica,
        (det_pessoa_fisica.nome == mgp_pessoa_fisica.PESF_NM_PESSOA_FISICA)
        & (det_pessoa_fisica.data_nascimento == mgp_pessoa_fisica.PESF_DT_NASC)
    )

    pessoa_fisica_nome_rg_mae = det_pessoa_fisica.join(
        mgp_pessoa_fisica,
        (det_pessoa_fisica.nome_rg == mgp_pessoa_fisica.PESF_NM_PESSOA_FISICA)
        & (det_pessoa_fisica.nome_mae == mgp_pessoa_fisica.PESF_NM_MAE)
    )

    pessoa_fisica_nome_rg_mae_rg = det_pessoa_fisica.join(
        mgp_pessoa_fisica,
        (lower(det_pessoa_fisica.nome_rg) == lower(
            mgp_pessoa_fisica.PESF_NM_PESSOA_FISICA))
        & (lower(
            det_pessoa_fisica.nome_mae_rg) == lower(
                mgp_pessoa_fisica.PESF_NM_MAE))
    )

    pessoa_fisica_nome_rg_nasc = det_pessoa_fisica.join(
        mgp_pessoa_fisica,
        (det_pessoa_fisica.nome_rg == mgp_pessoa_fisica.PESF_NM_PESSOA_FISICA)
        & (det_pessoa_fisica.data_nascimento == mgp_pessoa_fisica.PESF_DT_NASC)
    )

    pessoa_fisica = pessoa_fisica_cpf.\
        union(pessoa_fisica_rg).\
        union(pessoa_fisica_nome_mae).\
        union(pessoa_fisica_nome_mae_rg).\
        union(pessoa_fisica_nome_nasc).\
        union(pessoa_fisica_nome_rg_mae).\
        union(pessoa_fisica_nome_rg_mae_rg).\
        union(pessoa_fisica_nome_rg_nasc).\
        select(['uuid', 'pesf_pess_dk']).\
        distinct()

    resultado = pessoa_fisica.withColumnRenamed('uuid', 'start_node').\
        join(
            personagem,
            pessoa_fisica.pesf_pess_dk == personagem.PERS_PESS_DK
        ).withColumnRenamed('uuid', 'end_node').\
        select(['start_node', 'end_node']).\
        withColumn('label', lit('PERSONAGEM').cast(StringType())).\
        withColumn('uuid', uuidshaudf())

with Database(DADOSSINAPSE):
    resultado.write.mode("overwrite").saveAsTable(
        "pessoa_personagem_ope")

    personagem_documento = personagem.withColumnRenamed('uuid', 'start_node').\
        join(documentos, personagem.PERS_DOCU_DK == documentos.docu_dk).\
        select(['start_node', 'uuid']).\
        withColumnRenamed('uuid', 'end_node').\
        withColumn('label', lit('PERSONAGEM').cast(StringType())).\
        withColumn('uuid', uuidshaudf())

    personagem_documento.write.mode("overwrite").saveAsTable(
        "personagem_documento_ope")

    documento_orgao = documentos.withColumnRenamed('uuid', 'start_node').\
        join(
            orgaos,
            documentos.docu_orgi_orga_dk_responsavel == orgaos.cd_orgao).\
        select(['start_node', 'uuid']).\
        withColumnRenamed('uuid', 'end_node').\
        withColumn('label', lit('ORGAO_RESPONSAVEL').cast(StringType())).\
        withColumn('uuid', uuidshaudf())

    documento_orgao.write.mode("overwrite").saveAsTable(
        "documento_orgao_ope")

    mprj = empresas.select(['uuid']).\
        withColumnRenamed('uuid', 'end_node').\
        filter('num_cnpj = 28305936000140')

    orgao_mprj = orgaos.withColumnRenamed('uuid', 'start_node').\
        select(['start_node']).\
        withColumn(
            'end_node',
            lit(mprj.collect()[0].end_node).cast(StringType())).\
        withColumn('label', lit('PARTE_DE').cast(StringType())).\
        withColumn('uuid', uuidshaudf())

    orgao_mprj.write.mode("overwrite").saveAsTable(
        "orgao_mprj_ope")
