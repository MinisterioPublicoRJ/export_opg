# -*- coding: utf-8 -*-
from base import spark
from timer import timer
from pyspark.sql.functions import col
from opg_utils import uuidsha, update_uuid


uuidshaudf = spark.udf.register('uuidshaudf', uuidsha)

print('Generating People UUID')
with timer():
    update_uuid(
        'staging.lc_cpf',
        spark,
        'bases.lc_cpf',
        uuidshaudf(
            col('num_cpf'),
            col('nome'),
            col('data_nascimento').cast('string'),
            col('ind_sexo'),
            col('nome_mae')
        )
    )

print('Generating Detran Registro Civil UUID')
with timer():
    regcivil_no_header = spark.sql("""
       SELECT * FROM staging.detran_regcivil
       WHERE base = 'DETRAN'
   """)
    regcivil_no_header.registerTempTable('regcivil_no_header')
    update_uuid(
        'regcivil_no_header',
        spark,
        'bases.detran_regcivil',
        uuidshaudf(
            col('base'),
            col('nu_rg'),
            col('dt_expedicao_carteira').cast('string'),
            col('no_cidadao'),
            col('no_paicidadao'),
            col('no_maecidadao'),
            col('naturalidade'),
            col('dt_nascimento').cast('string'),
            col('documento_origem'),
            col('nu_cpf'),
            col('endereco'),
            col('bairro'),
            col('municipio'),
            col('uf'),
            col('cep')
        )
    )

print('Generating Companys UUID')
with timer():
    update_uuid(
        'staging.lc_cnpj',
        spark,
        'bases.lc_cnpj',
        uuidshaudf(
            col('num_cnpj'),
            col('ind_matriz_filial').cast('string'),
            col('nome'),
        )
    )

print('Generating Partnership UUID')
with timer():
    update_uuid(
        'staging.lc_socio',
        spark,
        'bases.lc_socio',
        uuidshaudf(
            col('cnpj'),
            col('cpf_socio'),
            col('dt_inicio').cast('string'),
            col('dt_fim').cast('string'),
        )
    )

print('Generating Work UUID')
with timer():
    update_uuid(
        'staging.lc_vinculo_trabalhista',
        spark,
        'bases.lc_vinculo_trabalhista',
        uuidshaudf(
            col('cnpj'),
            col('cpf'),
            col('dt_inicio').cast('string'),
            col('dt_fim').cast('string'),
        )
    )

print('Generating Ship UUID')
with timer():
    update_uuid(
        'staging.lc_embarcacao',
        spark,
        'bases.lc_embarcacao',
        uuidshaudf(
            col('id_embarcacao').cast('string'),
            col('ds_nome_embarcacao'),
            col('tipo_embarcacao'),
            col('cpf_cnpj'),
            col('situacao_embarcacao'),
            col('ult_obs_imp_doc_emb_gr_porte'),
            col('propr_armador_afret_atual'),
            col('construtor_casco'),
        )
    )

print('Generating Detran Multa UUID')
with timer():
    multa_no_header = spark.sql("""
       SELECT * FROM staging.detran_multa
       WHERE cd_org_aut != 'CD_ORG_AUT'
   """)
    multa_no_header.registerTempTable('multa_no_header')
    update_uuid(
        'multa_no_header',
        spark,
        'bases.detran_multa',
        uuidshaudf(
            col('aa_inf').cast('string'),
            col('cd_org_aut'),
            col('ds_org_aut'),
            col('autoinfra'),
            col('id_ntf_ar_aut_sm'),
            col('pl_vei_inf'),
            col('cd_inf'),
            col('dv_cd_inf'),
            col('desd_cd_inf'),
            col('ds_inf_tab'),
            col('tp_enq_tab'),
            col('pto_inf_tab'),
            col('cd_cls_agt'),
            col('nu_agt_inf'),
            col('dt_inf').cast('string'),
            col('hr_inf').cast('string'),
            col('localinfra'),
            col('tve_tab_descricao_marca'),
            col('descricao_especie'),
            col('descricao_categorias'),
            col('descricao_tipo'),
            col('descricao_cor'),
            col('dt_status_aut_sm').cast('string')
        )
    )

print('Generating Detran Veiculo UUID')
with timer():
    veiculo_no_header = spark.sql("""
       SELECT * FROM staging.detran_veiculo
       WHERE placa != 'PLACA'
   """)
    veiculo_no_header.registerTempTable('veiculo_no_header')
    update_uuid(
        'veiculo_no_header',
        spark,
        'bases.detran_veiculo',
        uuidshaudf(
            col('placa'),
            col('renavam'),
            col('chassi')
        )
    )

print('Generating Personagem UUID')
with timer():
    personagens = spark.sql("""
        select
            tipo.tppe_descricao,
            pessoa.pess_id_cadastro_receita cpfcnpj,
            pessoa.pess_nm_pessoa,
            personagem.*
        from exadata.mcpr_personagem personagem
        inner join exadata.mcpr_tp_personagem tipo on
            tipo.tppe_dk = personagem.pers_tppe_dk
        left join exadata.mcpr_pessoa pessoa on
            pessoa.pess_dk = personagem.pers_pess_dk
    """)
    personagens.registerTempTable('personagem')
    update_uuid(
        'personagem',
        spark,
        'bases.personagem',
        uuidshaudf(
            col('pers_dk').cast('string'),
            col('pers_pess_dk').cast('string'),
            col('pers_tppe_dk').cast('string')
        ),
        800
    )

print('Generating Orgao UUID')
with timer():
    orgaos = spark.sql("""
   select
       cd_orgao,
       nm_orgao,
       nm_regiao,
       nm_comarca,
       nm_foro,
       nm_tporgao
   from exadata_views.orgi_vw_orgao_gt
   """)
    orgaos.registerTempTable('orgaos')
    update_uuid(
        'orgaos',
        spark,
        'bases.orgaos',
        uuidshaudf(
            col('cd_orgao').cast('string'),
            col('nm_orgao'),
        ),
    )
    orgaos.unpersist()

print('Generating Document UUID')
with timer():
    documentos = spark.sql("""
   select
       docu_dk,
       docu_nr_externo,
       docu_ano,
       documento.docu_orgi_orga_dk_responsavel,
       documento.docu_nr_mp,
       documento.docu_dt_cadastro,
       hierarquia.cldc_ds_hierarquia
   from exadata.mcpr_documento documento
   inner join exadata_aux.mmps_classe_hierarquia hierarquia on
       documento.docu_cldc_dk = hierarquia.cldc_dk
   """)
    documentos.registerTempTable('documentos')
    update_uuid(
        'documentos',
        spark,
        'bases.documentos',
        uuidshaudf(
            col('docu_dk').cast('string'),
            col('docu_nr_externo').cast('string'),
            col('docu_orgi_orga_dk_responsavel').cast('string')
        ),
    )
    documentos.unpersist()
