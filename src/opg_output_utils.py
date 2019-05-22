from datetime import datetime
from pyspark.sql import Row
from hdfs import InsecureClient
from os.path import join as pathjoin


hdfsclient = InsecureClient('http://bda1node06:14000', 'mpmapas')


def list_output_csv(path):
    _path = path.replace('hdfs://', '')
    return [pathjoin(path, name) for name in filter(
        lambda x: '.csv' in x, hdfsclient.list(_path))]


def row_to_opv(label_entidade):
    def wrapper(row):
        row = row.asDict()
        uuid = row.pop('uuid')
        returns = []

        newrow = {}
        newrow['uuid'] = uuid
        newrow['label'] = 'label'
        newrow['type'] = 1
        newrow['svalue'] = label_entidade
        newrow['ilvalue'] = None
        newrow['datvalue'] = None
        returns.append(Row(**newrow))

        for key in row:
            newrow = {}
            svalue = None
            ilvalue = None
            tvalue = row[key]
            vtype = type(tvalue)

            if vtype == float:
                ptype = 4
                ilvalue = tvalue
            elif vtype == int:
                ptype = 2
                ilvalue = tvalue
            elif vtype == datetime:
                ptype = 1
                svalue = tvalue.isoformat()
            else:
                ptype = 1
                svalue = tvalue

            newrow['uuid'] = uuid
            newrow['label'] = key
            newrow['type'] = ptype
            newrow['svalue'] = svalue
            newrow['ilvalue'] = ilvalue
            newrow['datvalue'] = None
            returns.append(Row(**newrow))
        return returns

    return wrapper


def row_to_ope(label_entidade):
    def wrapper(row):
        row = row.asDict()
        uuid = row.pop('uuid')
        start_node = row.pop('start_node')
        end_node = row.pop('end_node')
        elabel = row.pop('label')
        returns = []

        baserow = {
            'uuid': uuid,
            'start_node': start_node,
            'end_node': end_node,
            'elabel': elabel,
            'label': None,
            'type': None,
            'svalue': None,
            'ilvalue': None,
            'datvalue': None,
        }

        newrow = dict(baserow)
        newrow['label'] = 'label'
        newrow['type'] = 1
        newrow['svalue'] = label_entidade
        returns.append(Row(**newrow))

        for key in row:
            newrow = dict(baserow)
            svalue = None
            ilvalue = None
            tvalue = row[key]
            vtype = type(tvalue)

            if vtype == float:
                ptype = 4
                ilvalue = tvalue
            elif vtype == int:
                ptype = 2
                ilvalue = tvalue * 1.0
            elif vtype == datetime:
                ptype = 1
                svalue = tvalue.isoformat()
            else:
                ptype = 1
                svalue = tvalue

            newrow['label'] = key
            newrow['type'] = ptype
            newrow['svalue'] = svalue
            newrow['ilvalue'] = ilvalue
            newrow['datvalue'] = None
            returns.append(Row(**newrow))

        if returns:
            return returns

        returns.append(Row(**baserow))

        return returns

    return wrapper


PGX_TYPES = {
    '1': 'string',
    '2': 'double',
    '4': 'string'
}


def translate_opx_type(columns):
    return [
        {
            'name': column[0],
            'type': PGX_TYPES[str(column[1])]
        }
        for column in columns
    ]


def generate_output_properties(medium, dest_file, columns):
    output = {}
    output['columns'] = translate_opx_type(
        medium.select(['label', 'type']).limit(
            20
        ).drop_duplicates().rdd.map(lambda x: [x[0], x[1]]).collect()
    )
    output['files'] = list_output_csv(dest_file)

    return output


def generate_opv(table, dest_file, spark, label):
    medium = spark.table(table)\
        .rdd\
        .flatMap(row_to_opv(label)).toDF(
            schema="""
            uuid string,
            label string,
            type int,
            svalue string,
            ilvalue float,
            datvalue string
        """).orderBy(['uuid', 'label'])

    medium.write\
        .mode('overwrite')\
        .csv(dest_file)

    return generate_output_properties(
        medium,
        dest_file,
        ['label', 'type']
    )


def generate_ope(table, dest_file, spark, label):
    medium = spark.table(table)\
        .rdd\
        .flatMap(row_to_ope(label)).toDF(
            schema="""
                uuid string,
                start_node string,
                end_node string,
                elabel string,
                label string,
                type int,
                svalue string,
                ilvalue float,
                datvalue string
            """).orderBy(
                ['uuid', 'start_node', 'end_node', 'elabel', 'label']
            )

    medium.write\
        .mode('overwrite')\
        .csv(dest_file)

    return generate_output_properties(
        medium,
        dest_file,
        ['elabel', 'type']
    )
