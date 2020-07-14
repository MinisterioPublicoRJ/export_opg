from base import spark
from hdfs import InsecureClient


client = InsecureClient('http://bda1node06:14000', user='felipeferreira')


def save_header(header, dest_folder):
    formatted_path = "/".join(dest_folder.split('/')[5:])
    client.write(
        '{}/header/header.csv'.format(formatted_path),
        data=header,
        overwrite=True)


def generate_node_csv_for_neo4j(destination, table_name, n_partitions=1):
    dest_folder = '{}/nodes/{}'.format(destination, table_name)

    data = spark.sql("""
        SELECT * FROM {}
    """.format(table_name)).coalesce(n_partitions)

    data.write\
        .mode('overwrite')\
        .option('quote', '"')\
        .option('escape', '"')\
        .csv("{}/data".format(dest_folder))

    header = []

    for col_name in data.columns:
        if col_name == 'uuid':
            header.append(col_name + ':ID')
        elif col_name == 'label':
            header.append(col_name + ':Label')
        else:
            header.append(col_name)

    header = ",".join(header)
    save_header(header, dest_folder)


def generate_relationship_csv_for_neo4j(
        destination,
        table_name,
        n_partitions=1):
    dest_folder = '{}/relationships/{}'.format(destination, table_name)

    data = spark.sql("""
        SELECT * FROM {}
    """.format(table_name)).coalesce(n_partitions)

    data.write\
        .mode('overwrite')\
        .option('quote', '"')\
        .option('escape', '"')\
        .csv("{}/data".format(dest_folder))

    header = []

    for col_name in data.columns:
        if col_name == 'start_node':
            header.append(col_name + ':START_ID')
        elif col_name == 'end_node':
            header.append(col_name + ':END_ID')
        elif col_name == 'label':
            header.append(col_name + ':TYPE')
        else:
            header.append(col_name)

    header = ",".join(header)
    save_header(header, dest_folder)
