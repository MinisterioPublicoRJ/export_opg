from base import spark
from timer import timer
from opg_output_utils import generate_ope, generate_opv
from json import dumps

v_outputs = []
e_outputs = []

print('Generating People OPV file')
with timer():
    v_outputs.append(
        generate_opv(
            'dadossinapse.pessoa_fisica_opv',
            'hdfs:///user/felipeferreira/dadossinapse/ops/pessoa_fisica.opv',
            spark,
            'pessoa'
        )
    )

print('Generating Companys OPV file')
with timer():
    v_outputs.append(
        generate_opv(
            'dadossinapse.pessoa_juridica_opv',
            'hdfs:///user/felipeferreira/dadossinapse/ops/pessoa_juridica.opv',
            spark,
            'empresa'
        )
    )

print('Generating Contributory OPE file')
with timer():
    e_outputs.append(
        generate_ope(
            'dadossinapse.socio_responsavel_ope',
            'hdfs:///user/felipeferreira/'
            'dadossinapse/ops/socio_responsavel.ope',
            spark, 'socioresponsavel'
        )
    )

print('Generating Partnership OPE file')
with timer():
    e_outputs.append(
        generate_ope(
            'dadossinapse.socio_ope',
            'hdfs:///user/felipeferreira/dadossinapse/ops/socio.ope',
            spark,
            'socio'
        )
    )

print('Generating Motherhood OPE file')
with timer():
    e_outputs.append(
        generate_ope(
            'dadossinapse.maternidade_ope',
            'hdfs:///user/felipeferreira/dadossinapse/ops/maternidade.ope',
            spark,
            'mae'
        )
    )

print('Generating Work OPE file')
with timer():
    e_outputs.append(
        generate_ope(
            'dadossinapse.vinculo_trabalhista_ope',
            'hdfs:///user/felipeferreira/dadossinapse'
            '/ops/vinculo_trabalhista.ope',
            spark,
            'trabalha'
        )
    )

base_format = {
  "format": "flat_file",
  "separator": ",",
  "edge_uri_list": [item for it in e_outputs for item in it['files']],
  "vertex_uri_list": [item for it in v_outputs for item in it['files']],
  "vertex_props": [
    item for it in v_outputs for item in it['columns']
  ],
  "edge_props": [
    item for it in e_outputs for item in it['columns']
  ],
  "edge_label": True,
  "loading": {"load_edge_label": True}
}

with open('pgx_props.json', 'w+') as file:
    file.write(dumps(base_format))
