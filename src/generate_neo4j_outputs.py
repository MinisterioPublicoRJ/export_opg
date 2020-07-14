from timer import timer
from neo4j_output_utils import (
    generate_node_csv_for_neo4j,
    generate_relationship_csv_for_neo4j
)
from base import DADOSSINAPSE, DESTFOLDER
from context import Database

dest_folder = "hdfs:///user/felipeferreira/%s/neo4j" % DESTFOLDER

with Database(DADOSSINAPSE):
    print('Generating Pessoa nodes for neo4j')
    with timer():
        generate_node_csv_for_neo4j(
            destination=dest_folder,
            table_name="pessoa_fisica_opv"
        )

    print('Generating Empresa nodes for neo4j')
    with timer():
        generate_node_csv_for_neo4j(
            destination=dest_folder,
            table_name="pessoa_juridica_opv"
        )

    print('Generating TRABALHA relationships for neo4j')
    with timer():
        generate_relationship_csv_for_neo4j(
            destination=dest_folder,
            table_name="vinculo_trabalhista_ope"
        )

    print('Generating SOCIO_RESPONSAVEL relationships for neo4j')
    with timer():
        generate_relationship_csv_for_neo4j(
            destination=dest_folder,
            table_name="socio_responsavel_ope"
        )

    print('Generating SOCIO relationships for neo4j')
    with timer():
        generate_relationship_csv_for_neo4j(
            destination=dest_folder,
            table_name="socio_ope"
        )

    print('Generating FILHO relationships for neo4j (maternidade)')
    with timer():
        generate_relationship_csv_for_neo4j(
            destination=dest_folder,
            table_name="pessoa_mae_ope"
        )

    print('Generating FILHO relationships for neo4j (paternidade)')
    with timer():
        generate_relationship_csv_for_neo4j(
            destination=dest_folder,
            table_name="pessoa_pai_ope"
        )

    print('Generating Documento nodes for neo4j')
    with timer():
        generate_node_csv_for_neo4j(
            destination=dest_folder,
            table_name="documento_opv"
        )

    print('Generating Multa nodes for neo4j')
    with timer():
        generate_node_csv_for_neo4j(
            destination=dest_folder,
            table_name="detran_multa_opv"
        )

    print('Generating Veiculo nodes for neo4j')
    with timer():
        generate_node_csv_for_neo4j(
            destination=dest_folder,
            table_name="detran_veiculo_opv"
        )

    print('Generating Embarcacao nodes for neo4j')
    with timer():
        generate_node_csv_for_neo4j(
            destination=dest_folder,
            table_name="embarcacao_opv"
        )

    print('Generating PROPRIETARIO relationships for neo4j (pessoa-veiculo)')
    with timer():
        generate_relationship_csv_for_neo4j(
            destination=dest_folder,
            table_name="pessoa_veiculo_ope"
        )

    print('Generating AUTUADO relationships for neo4j (veiculo-multa)')
    with timer():
        generate_relationship_csv_for_neo4j(
            destination=dest_folder,
            table_name="veiculo_multa_ope"
        )

    print('Generating AUTUADO relationships for neo4j (pessoa-multa)')
    with timer():
        generate_relationship_csv_for_neo4j(
            destination=dest_folder,
            table_name="pessoa_multa_ope"
        )

    print('Generating AUTUADO relationships for neo4j (empresa-multa)')
    with timer():
        generate_relationship_csv_for_neo4j(
            destination=dest_folder,
            table_name="empresa_multa_ope"
        )

    print('Generating PERSONAGEM relationships for neo4j (pessoa-personagem)')
    with timer():
        generate_relationship_csv_for_neo4j(
            destination=dest_folder,
            table_name="pessoa_personagem_ope"
        )

    print('Generating PERSONAGEM relationships for neo4j (personagem-documento)')
    with timer():
        generate_relationship_csv_for_neo4j(
            destination=dest_folder,
            table_name="personagem_documento_ope"
        )

    print('Generating PARTE_DE relationships for neo4j (orgao-mprj)')
    with timer():
        generate_relationship_csv_for_neo4j(
            destination=dest_folder,
            table_name="orgao_mprj_ope"
        )

    print('Generating ORGAO_RESPONSAVEL relationships for neo4j')
    with timer():
        generate_relationship_csv_for_neo4j(
            destination=dest_folder,
            table_name="documento_orgao_ope"
        )

    print('Generating PROPRIETARIO relationships for neo4j (pessoa-embarcacao)')
    with timer():
        generate_relationship_csv_for_neo4j(
            destination=dest_folder,
            table_name="pessoa_embarcacao_ope"
        )
    print('Generating PROPRIETARIO relationships for neo4j')
    with timer():
        generate_relationship_csv_for_neo4j(
            destination=dest_folder,
            table_name="empresa_embarcacao_ope"
        )

    print('Generating Personagem nodes for neo4j')
    with timer():
        generate_node_csv_for_neo4j(
            destination=dest_folder,
            table_name="personagem_opv"
        )

    print('Generating Orgao nodes for neo4j')
    with timer():
        generate_node_csv_for_neo4j(
            destination=dest_folder,
            table_name="orgao_opv"
        )
