
## Index script for HMDB database

* [index.py](index.py) indexes HMDB proteins and metabolites datasets,
tests made with HMDB version 4.0
 

* [../tests/query-hmdb-kbase.py](../tests/query-hmdb-kbase.py)
 includes few sample queries


#### HMDB downloads page

http://www.hmdb.ca/downloads

```bash
mkdir -p data
wget -P ./data http://www.hmdb.ca/system/downloads/current/hmdb_metabolites.zip
wget -P ./data http://www.hmdb.ca/system/downloads/current/hmdb_proteins.zip

./hmdb/index.py --infile ./data/hmdb_metabolites.zip --db Elasticsearch
./hmdb/index.py --infile ./data/hmdb_proteins.zip --db Elasticsearch
./hmdb/index.py --infile ./data/hmdb_metabolites.zip --db MongoDB
./hmdb/index.py --infile ./data/hmdb_proteins.zip --db MongoDB
```

