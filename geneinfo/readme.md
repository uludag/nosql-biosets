
## HGNC gene info

[hgnc_geneinfo.py](hgnc_geneinfo.py); Index HGNC data using Elasticsearch,
 MongoDB or PostgresSQL

```bash
mkdir -p data
wget -P ./data http://ftp.ebi.ac.uk/pub/databases/genenames/new/json/hgnc_complete_set.json

./geneinfo/hgnc_geneinfo.py --infile ./data/hgnc_complete_set.json --db Elasticsearch

./geneinfo/hgnc_geneinfo.py --infile ./data/hgnc_complete_set.json --db MongoDB

# Assumes PostgresSQL database with name geneinfo has already been created
# and the user `tests` have access to the database with password 'tests' 
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
