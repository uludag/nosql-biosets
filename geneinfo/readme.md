
## HGNC gene info

[hgnc_geneinfo.py](hgnc_geneinfo.py); Index HGNC data using Elasticsearch,
 MongoDB or PostgreSQL(naive)
 
 Tested with Mar 2019 release

```bash
# ~30M
wget -nc -P ./data http://ftp.ebi.ac.uk/pub/databases/genenames/new/json/hgnc_complete_set.json

# Requires ~1m
./geneinfo/hgnc_geneinfo.py --infile ./data/hgnc_complete_set.json --db Elasticsearch

# Requires ~10s
./geneinfo/hgnc_geneinfo.py --infile ./data/hgnc_complete_set.json --db MongoDB\
 --index biosets

# Requires ~1m
# Assume PostgreSQL database with name geneinfo has already been created
# and the user `geneinfo` have access to the database with password 'geneinfo'
# Use --hosts and  --port options if the database host is different than localhost
# or if its port number is different than 5432
./geneinfo/hgnc_geneinfo.py --infile ./data/hgnc_complete_set.json\
 --db PostgreSQL --index geneinfo --user geneinfo --password geneinfo
```
PostgreSQL support is based on [SQLAlchemy](http://www.sqlalchemy.org) library,
table name is defined with constant `DOCTYPE` ('hgncgeneinfo')

### Similar work

* https://github.com/LeKono/pyhgnc


## RNAcentral id mappings

Tested with RNAcentral Release 8, Dec 2017

```bash
mkdir -p data
# ~300M
wget -P ./data http://ftp.ebi.ac.uk/pub/databases/RNAcentral/current_release/id_mapping/id_mapping.tsv.gz
# Requires ~50m
./geneinfo/rnacentral_idmappings.py --infile ./data/id_mapping.tsv.gz --db Elasticsearch\
 --index rnacentral
# Requires ~25m (~10m for inserts, ~15m for text/field indicies)
./geneinfo/rnacentral_idmappings.py --infile ./data/id_mapping.tsv.gz --db MongoDB\
 --index biosets
```


## Ensembl regulatory build

In this folder we also have Elasticsearch [indexer](ensembl_regbuild.py) for Ensembl
regulatory build GFF files which is at its early stages of development.
GFF files are parsed by the [gffutils](https://github.com/daler/gffutils)
library.

```
./geneinfo/ensembl_regbuild.py --help
usage: ensembl_regbuild.py [-h] [--infile INFILE] [--index INDEX]
                           [--gfftype GFFTYPE] [--db DB] [--host HOST]
                           [--port PORT]

Index Ensembl regulatory build gff files using Elasticsearch

optional arguments:
  -h, --help         show this help message and exit
  --infile INFILE    Transcription factors binding sites or Regulatory regions
                     gff file
  --index INDEX      Name of the Elasticsearch index
  --gfftype GFFTYPE  Type of the gff file, should be "transcriptionfactor" or
                     "regulatoryregion"
  --db DB            Database: only 'Elasticsearch' is supported
  --host HOST        Elasticsearch server hostname
  --port PORT        Elasticsearch server port
```
