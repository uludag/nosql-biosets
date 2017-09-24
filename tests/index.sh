#!/usr/bin/env bash
# Script for indexing the datasets supported by the project
# Note: not all supported datasets have been included in this script yet
echo `dirname $0`
r=`dirname $0`/..    # Project root folder
index=nosqlbiosets   # Elasticsearch index name or MongoDB database name
dbs="MongoDB Elasticsearch"

for db in ${dbs}; do
    echo "Indexing with database '${db}'"

## MetaNetX
#  Typical indexing time for compounds & reactions files,
#  Elasticsearch 10-15m, MongoDB ~20m
#   ./metanetx/index.py --metanetxdatafolder ./metanetx/data/\
#        --index ${index} --db ${db};


## KBase
#  Typical indexing time ??
#    ${r}/nosqlbiosets/kbase/index.py\
#        --compoundsfile ${r}/data/kbase/compounds.csv\
#        --reactionsfile ${r}/data/kbase/reactions.csv\
#        --index ${index} --db ${db};


## HMDB
# Typical total indexing time for all metabolites and proteins .zip files: ~110m
#    for infile in `ls ${r}/data/hmdb/3.6/*.zip`; do
#        ${r}/hmdb/index.py --infile ${infile}\
#            --index ${index} --db ${db};
#    done


## UniProt (recent work)
# Typical Elasticsearch indexing times 5h to 8h
# Typical MongoDB indexing times 2.5h to 5h
# ./nosqlbiosets/uniprot/index.py\
#   --infile ./data/uniprot_sprot.xml.gz --index ${index} --db ${db}


## KEGG (work in progress)
# Typical Elasticsearch indexing time ??
# Typical MongoDB indexing time ??
# ./nosqlbiosets/kegg/index.py\
#   --infile ./data/kegg/xml/kgml/metabolic/organisms/\
#   --index ${index} --db ${db}

done


# Sample PMC articles, Elasticsearch only, indexing time about 20m
#./index-pmc-articles.py --infile ./data/PMC0044-1000.tar --index ${index}
