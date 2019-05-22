from base import spark, sc
from opg_utils import uuidsha
from timer import timer
import time
import requests
import json
from pyspark.sql.functions import lit
import pysolr
import csv
import StringIO


def chunkeia(generator, n):                                                                                   
    conta = 0                                                                                      
    saida = False                                                                                             
    bloco = []                                                                  
    for i in generator:                                                         
        bloco.append(i)                                                                                       
        conta += 1                                                                                            
        if conta >= n:                                                                     
            yield bloco                                                                         
            bloco = []                                                                                        
            conta = 0                                                                               
    if bloco:                                                                                                 
        yield bloco
 

def ipartition(iterable, partition_size):
    from collections import Iterator
    if not isinstance(iterable, Iterator):
        iterator = iter(iterable)
    else:
        iterator = iterable

    finished = False
    while not finished:
        data = []
        for _ in range(partition_size):
            try:
                data.append(next(iterator))
            except StopIteration:
                finished = True
                break
        if data:
            yield data


def commit_pysolr(colecao):
    endpoint = cria_solr(colecao, 300)
    endpoint.add(
        [],
        commit=True
    )


def post_pysolr(listas, endpoint, debug=False):
    json_envio = [item.asDict() for item in listas]

    for i in range(3):
        try:
            endpoint.add(
                json_envio,
                commit=False
            )
            break
        except:
            time.sleep(5)
            continue


def cria_solr(colecao, timeout=40):
    zookeeper = pysolr.ZooKeeper("bda1node05:2181,bda1node06:2181,bda1node07:2181/solr")
    return pysolr.SolrCloud(zookeeper, colecao, always_commit=False, timeout=timeout)


def processaparticao(colecao):
    def _processaparticao(iterador):
        endpoint = cria_solr(colecao)
        for grupo in ipartition(iterador, 100000):
            post_pysolr(grupo, endpoint)

        return [0]

    return _processaparticao


colecoes = [
    {
        'colecao': 'veiculo',
        'debug': True,
        'query':"""
            select
                proprietario,
                placa,
                renavam,
                marca_modelo,
                descricao_cor cor,
                cpfcgc cpfcnpj,
                fabric ano_fabric,
                modelo ano_modelo,
                chassi,
                proprietario,
                uuid,
                'Veiculo' label,
                concat(
                    trim(marca_modelo),
                    ' ',
                    trim(descricao_cor),
                    ' ',
                    fabric,
                    '/',
                    modelo,
                    ' ',
                    placa
                ) descricao
            from bases.detran_veiculo
        """
    },
    {
        'colecao': 'documento_personagem',
        'query': """
            SELECT  d.uuid,
                d.cdorgao,
                d.cldc_ds_hierarquia,
                d.docu_dk,
                d.dt_cadastro,
                d.nr_externo,
                d.nr_mp, 
                collect_set(p.tp_descricao) as ds_info_personagem,
                'Documento' as label
            FROM dadossinapse.documento_opv d
            join (
                select p.pers_docu_dk,  concat(p.tppe_descricao, ' - ', p.cpfcnpj, ' - ', p.pess_nm_pessoa) as tp_descricao
                from dadossinapse.personagem_opv p
            ) p
                on p.pers_docu_dk = d.docu_dk
            GROUP BY d.uuid,
                d.cdorgao,
                d.cldc_ds_hierarquia,
                d.docu_dk,
                d.dt_cadastro,
                d.nr_externo,
                d.nr_mp
        """
    },
    {
        'colecao': 'embarcacao',
        'query': """
            select 
                uuid,
                ds_nome_embarcacao as nome_embarcacao,
                tipo_embarcacao,
                ano_construcao,
                cpf_cnpj,
                'Embarcacao' label
            from bases.lc_embarcacao
        """
    },
    {
        'colecao': 'pessoa_juridica',
        'query': """
            select 
                lc_cnpj.uuid,
                nome_fantasia,
                lc_cnpj.nome razao_social,
                lc_cpf.nome responsavel,
                lc_cnpj.num_cnpj cnpj,
                num_cpf_responsavel cpf_responsavel,
                lc_cnpj.nome_municipio municipio,
                lc_cnpj.sigla_uf uf,
                'Empresa' label
            from bases.lc_cnpj
            inner join bases.lc_cpf on
                lc_cnpj.num_cpf_responsavel = lc_cpf.num_cpf
        """
    },
    {
        'colecao': 'pessoa_fisica',
        'query': """
            SELECT 
                pessoa_fisica_opv.uuid uuid,
                pessoa_fisica_opv.num_cpf cpf,
                pessoa_fisica_opv.num_rg rg,
                concat( date_format(pessoa_fisica_opv.data_nascimento, 'yyyy-MM-dd'), 'T00:00:00Z') as dt_nasc,
                pessoa_fisica_opv.nome nome,
                pessoa_fisica_opv.nome_rg nome_rg,
                pessoa_fisica_opv.nome_mae nome_mae,
                pessoa_fisica_opv.nome_pai nome_pai,
                case pessoa_fisica_opv.ind_sexo
                when 'M' then 'Masculino'
                when 'F' then 'Feminino'
                else 'Indisponivel' end as sexo,
                pessoa_fisica_opv.sigla_uf uf,
                case 
                when lc_ppe.cpf is not null then true
                else false
                end as sensivel,
                'Pessoa' as label
            FROM dadossinapse.pessoa_fisica_opv 
            left join staging.lc_ppe on
                lc_ppe.cpf = pessoa_fisica_opv.num_cpf
        """
    }
]

for colecao in colecoes:
    print('Sending %s' % colecao['colecao'])
    with timer():
        tabela = spark.sql(colecao['query'])
        tabela = tabela.repartition(40)
        tabela.rdd.mapPartitions(processaparticao(colecao['colecao'])).count()
        commit_pysolr(colecao['colecao'])
