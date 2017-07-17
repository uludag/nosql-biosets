#!/usr/bin/env bash

# Script for indexing test datasets

for db in Elasticsearch MongoDB; do
    ## MetaNetX
    ./metanetx/index.py --compoundsfile ./metanetx/data/chem_prop-head.tsv\
        --reactionsfile ./metanetx/data/reac_prop-head.tsv\
        --index test --db ${db};

done

