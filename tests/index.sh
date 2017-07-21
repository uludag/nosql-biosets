#!/usr/bin/env bash

# Script for indexing test datasets
echo `dirname $0`
r=`dirname $0`/..    # Project root folder
index=nosqlbiosets   # Elasticsearch index name or MongoDB database name
dbs="MongoDB Elasticsearch"

for db in ${dbs}; do

## MetaNetX
#  Typical indexing time less than 30s for partial compounds/reactions files
#  ./metanetx/index.py --compoundsfile ./metanetx/data/chem_prop-head.tsv\
#        --reactionsfile ./metanetx/data/reac_prop-head.tsv\
#        --index ${index} --db ${db};
#  Typical indexing time for full compounds/reactions files less than 30m
#   ./metanetx/index.py --compoundsfile ./metanetx/data/chem_prop.tsv\
#        --reactionsfile ./metanetx/data/reac_prop.tsv\
#        --index ${index} --db ${db};

## KBase
#  Typical indexing time ??
#    ${r}/nosqlbiosets/kbase/index.py\
#        --compoundsfile ${r}/data/kbase/compounds.csv\
#        --reactionsfile ${r}/data/kbase/reactions.csv\
#        --index ${index} --db ${db};

## HMDB
#  Typical indexing time less than 30s
#    ./hmdb/index.py --infile ./data/hmdb_proteins-first10.xml.gz\
#        --index ${index} --db ${db};

# Typical total indexing time for 3 metabolite .zip files less than 30m
#    for infile in `ls ${r}/data/hmdb/*metab*.zip`; do
#        ./hmdb/index.py --infile ${infile}\
#            --index ${index} --db ${db};
#    done

# Typical total indexing time for 2 .zip files is about 110m
#    for infile in `ls ${r}/data/hmdb/3.6/*.zip`; do
#        ${r}/hmdb/index.py --infile ${infile}\
#            --index ${index} --db ${db};
#    done
done


# Sample PMC articles, Elasticsearch only, indexing time about 20m
#./index-pmc-articles.py --infile ./data/PMC0044-1000.tar --index ${index}
