
## Index script for HMDB database

* [index.py](index.py) Index HMDB proteins and metabolites datasets.

Tests made with HMDB version 4.0
 

* [../tests/test_hmdb_queries.py](../tests/test_hmdb_queries.py)
 Includes example queries


### Usage

```bash
# Download metabolites and proteins data
mkdir -p data
wget -P ./data http://www.hmdb.ca/system/downloads/current/hmdb_metabolites.zip
wget -P ./data http://www.hmdb.ca/system/downloads/current/hmdb_proteins.zip

# Index with Elasticsearch
./hmdb/index.py --infile ./data/hmdb_metabolites.zip --db Elasticsearch
./hmdb/index.py --infile ./data/hmdb_proteins.zip --db Elasticsearch

# Index with MongoDB
./hmdb/index.py --infile ./data/hmdb_metabolites.zip --db MongoDB
./hmdb/index.py --infile ./data/hmdb_proteins.zip --db MongoDB
```

