#!/bin/sh

#Usage statements
if [ "$#" = 0 ]; then
    echo "Usage: $0 csv_files_path"
    echo "\ncsv_files_path must be a path containing a 'nodes' and 'relationships' subdirectories."
    echo "\nThe subdirectories must contain folders, containing a 'data' and 'header' subfolders."
    echo "At least one node table must be present in the 'nodes' subdirectories."
    exit 1
fi

# Check that path is valid, with nodes and relationships subdirectories
# And that at least one node table is present
csv_files_path="${1}"
if [ ! -d "${csv_files_path}" ]; then
    >&2 echo "Path provided does not exist or is not a directory. Aborting operation..."
    exit 2
fi
if [ ! -d "${csv_files_path}/nodes" ]; then
    >&2 echo "Path provided does not contain a 'nodes' subdirectory. Aborting operation..."
    exit 2
fi
if [ ! -d "${csv_files_path}/relationships" ]; then
    >&2 echo "Path provided does not contain a 'relationships' subdirectory. Aborting operation..."
    exit 2
fi


# Check that there are nodes and relationships
# Get nodes and relationships parameters for the import command
for directory in ${csv_files_path}/nodes/*
do
    for header in ${directory}/header/*.csv
    do
        if [ -s "$header" ]; then

            n_id=`cat ${header} | grep -e ":ID" -o | wc -l`
            if [ $n_id = 1 ]; then
                :
            elif [ $n_id = 0 ]; then
                >&2 echo "ID column for $header: Missing! Aborting..."
                exit 3
            else
                >&2 echo "ID column for $header: More than one! Aborting..."
                exit 3
            fi
            n_label=`cat ${header} | grep -e ":Label" -o | wc -l`
            if [ $n_label = 1 ]; then
                :
            elif [ $n_label = 0 ]; then
                >&2 echo "Label column for $header: Missing! Aborting..."
                exit 3
            else
                >&2 echo "Label column for $header: More than one! Aborting..."
                exit 3
            fi

            node_files="${header}"
        else
            >&2 echo "No header, or empty header for $directory!"
            exit 8
        fi
    done

    for file in ${directory}/data/*.csv
    do
        if [ -s "$file" ]; then
            node_files="${node_files},${file}"
        fi
    done
    nodes="${nodes} --nodes=${node_files}"
done


for directory in ${csv_files_path}/relationships/*
do
    for header in ${directory}/header/*.csv
    do
        if [ -s "$header" ]; then

            n_start_id=`cat ${header} | grep -e ":START_ID" -o | wc -l`
            if [ $n_start_id = 1 ]; then
                :
            elif [ $n_start_id = 0 ]; then
                >&2 echo "START_ID column for $header: Missing! Aborting..."
                exit 3
            else
                >&2 echo "START_ID column for $header: More than one! Aborting..."
                exit 3
            fi

            n_end_id=`cat ${header} | grep -e ":END_ID" -o | wc -l`
            if [ $n_end_id = 1 ]; then
                :
            elif [ $n_end_id = 0 ]; then
                >&2 echo "END_ID column for $header: Missing! Aborting..."
                exit 3
            else
                >&2 echo "END_ID column for $header: More than one! Aborting..."
                exit 3
            fi

            n_type=`cat ${header} | grep -e ":TYPE" -o | wc -l`
            if [ $n_type = 1 ]; then
                :
            elif [ $n_type = 0 ]; then
                >&2 echo "TYPE column for $header: Missing! Aborting..."
                exit 3
            else
                >&2 echo "TYPE column for $header: More than one! Aborting..."
                exit 3
            fi

            relationship_files="${header}"
        else
            >&2 echo "No header, or empty header for $directory!"
            exit 8
        fi
    done

    for file in ${directory}/data/*.csv
    do
        if [ -s "$file" ]; then
            relationship_files="${relationship_files},${file}"
        fi
    done
    relationships="${relationships} --relationships=${relationship_files}"
done

# Generate script to run

echo "./bin/neo4j-admin import --database=temp_graph.db --ignore-duplicate-nodes=true --multiline-fields=true ${nodes} ${relationships}"
