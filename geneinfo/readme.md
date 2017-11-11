
## HGNC gene info

[hgnc_geneinfo.py](hgnc_geneinfo.py); Index HGNC data using Elasticsearch,
 MongoDB or PostgresSQL

```bash
mkdir -p data
wget -P ./data http://ftp.ebi.ac.uk/pub/databases/genenames/new/json/hgnc_complete_set.json

./geneinfo/hgnc_geneinfo.py --infile ./data/hgnc_complete_set.json --db Elasticsearch

./geneinfo/hgnc_geneinfo.py --infile ./data/hgnc_complete_set.json --db MongoDB

# Assume PostgresSQL database with name geneinfo has already been created
# and the user `tests` have access to the database with password 'tests'
# Use --hosts and  --port options if the database host is different than localhost
# or if its port number is different than 5432
./geneinfo/hgnc_geneinfo.py --infile ./data/hgnc_complete_set.json\
 --db PostgresSQL --index geneinfo --user tests --password tests
```
PostgresSQL support is based on [SQLAlchemy](http://www.sqlalchemy.org/) library

### Similar work

* https://github.com/LeKono/pyhgnc


## RNAcentral id mappings

```bash
mkdir -p data
wget -P ./data http://ftp.ebi.ac.uk/pub/databases/RNAcentral/current_release/id_mapping/id_mapping.tsv.gz
./geneinfo/rnacentral_idmappings.py --infile ./data/rnacentral-v7_id_mapping.tsv.gz --db Elasticsearch
./geneinfo/rnacentral_idmappings.py --infile ./data/rnacentral-v7_id_mapping.tsv.gz --db MongoDB

```

MongoDB index time; ~12m for inserts, ~15m for text/field indicies


## Ensembl regulatory build

In this folder we also have [indexer](ensembl_regbuild.py) for Ensembl
regulatory build GFF files which is at its early stages of development.
GFF files are parsed by using the [gffutils](https://github.com/daler/gffutils)
library.
