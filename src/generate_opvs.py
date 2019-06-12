from base import spark, BASES, DADOSSINAPSE
from timer import timer
from context import Database


print('Generating Peoples OPV')
with timer():
    print('Querying people attribute ')
    with timer(), Database(BASES):
        tabela = spark.sql("""from pessoa_fisica
            select uuid,
                num_cpf,
                data_nascimento,
                nome,
                nome_mae,
                ind_sexo,
                sigla_uf,
                num_rg,
                nome_rg,
                nome_pai,
                sensivel,
                'Pessoa' label
        """)

    print('Persisting people OPV')
    with timer(), Database(DADOSSINAPSE):
        tabela.write.mode('overwrite').saveAsTable(
            'pessoa_fisica_opv')

print('Generating Companys OPV')
with timer():
    print('Querying Company\'s atributes')
    with timer(), Database(BASES):
        tabela = spark.sql("""from lc_cnpj
            select
                uuid,
                nome_fantasia,
                num_cnpj,
                num_cpf_responsavel,
                data_abertura_estabelecimento,
                ind_matriz_filial,
                nome_municipio,
                sigla_uf,
                'Empresa' label
        """)

    print('Persisting Company\'s attributes')
    with timer(), Database(DADOSSINAPSE):
        tabela.write.mode('overwrite').saveAsTable(
            'pessoa_juridica_opv')

print('Generating Vehicle OPV')
with timer():
    print('Querying Vehicle\'s atributes')
    with timer(), Database(BASES):
        tabela = spark.sql("""
            select
                uuid,
                fabric,
                modelo,
                chassi,
                descricao_cor,
                cpfcgc,
                marca_modelo,
                placa,
                renavam,
                proprietario,
                'Veiculo' label
            from detran_veiculo
        """)

    print('Persisting Vehicle\'s attributes')
    with timer(), Database(DADOSSINAPSE):
        tabela.write.mode('overwrite').saveAsTable(
            'detran_veiculo_opv')

print('Generating Car Ticket OPV')
with timer():
    print('Querying Car Ticket\'s atributes')
    with timer(), Database(BASES):
        tabela = spark.sql("""
            select
                uuid,
                autoinfra,
                dt_inf datainfra,
                ds_inf_tab descinfra,
                ident2 cpfcnpj,
                nm_con_inf proprietario,
                pl_vei_inf placa,
                'Multa' label
            from detran_multa
        """)

    print('Persisting Car Ticket\'s attributes')
    with timer(), Database(DADOSSINAPSE):
        tabela.write.mode('overwrite').saveAsTable(
            'detran_multa_opv')

print('Generating Prosecutions OPV')
with timer():
    print('Querying Prosecution\'s atributes')
    with timer(), Database(BASES):
        tabela = spark.sql("""
            select
                uuid,
                docu_orgi_orga_dk_responsavel cdorgao,
                cldc_ds_hierarquia,
                docu_dk,
                docu_dt_cadastro dt_cadastro,
                docu_nr_externo nr_externo,
                docu_nr_mp nr_mp,
                'Documento' label
            from documentos
        """)

    print('Persisting Prosecution\'s attributes')
    with timer(), Database(DADOSSINAPSE):
        tabela.write.mode('overwrite').saveAsTable(
            'documento_opv')

print('Generating Watercrafts OPV')
with timer():
    print('Querying Watercraft\'s atributes')
    with timer(), Database(BASES):
        tabela = spark.sql("""
            select
                uuid,
                ds_nome_embarcacao nome_embarcacao,
                tipo_embarcacao,
                ano_construcao,
                nr_inscricao,
                cpf_cnpj,
                'Embarcacao' label
            from lc_embarcacao
        """)

    print('Persisting Watercraft\'s attributes')
    with timer(), Database(DADOSSINAPSE):
        tabela.write.mode('overwrite').saveAsTable(
            'embarcacao_opv')

print('Generating Organ OPV')
with timer():
    print('Querying Organ\'s atributes')
    with timer(), Database(BASES):
        tabela = spark.sql("""
            select
                uuid,
                cd_orgao,
                nm_orgao,
                nm_regiao,
                nm_comarca,
                nm_foro,
                nm_tporgao,
                'Orgao' label
            from orgaos
        """)

    print('Persisting Organ\'s attributes')
    with timer(), Database(DADOSSINAPSE):
        tabela.write.mode('overwrite').saveAsTable('orgao_opv')

print('Generating Character OPV')
with timer():
    print('Querying Character\'s atributes')
    with timer(), Database(BASES):
        tabela = spark.sql("""
            select
                uuid,
                pers_dk,
                pers_docu_dk,
                pers_dt_inicio,
                pers_dt_fim,
                'Personagem' label,
                tppe_descricao,
                cpfcnpj,
                pess_nm_pessoa
            from personagem""")

    print('Persisting Character\'s attributes')
    with timer(), Database(DADOSSINAPSE):
        tabela.write.mode('overwrite').saveAsTable(
            'personagem_opv')
