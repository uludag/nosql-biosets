
## Index/query scripts for HMDB and DrugBank xml datasets

* [index.py](index.py) Index HMDB proteins and metabolites datasets.

Tests made with HMDB version 4.0

* [../tests/test_hmdb_queries.py](../tests/test_hmdb_queries.py)
 Includes example queries

* [drugbank.py](drugbank.py) Index DrugBank xml dataset with MongoDB,
 or save drug interactions as NetworkX graph file

Tests made with DrugBank version 5.0

* [query.py](query.py) Query DrugBank dataset indexed with MongoDB


### Usage HMDB

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


### Usage DrugBank

```bash
mkdir -p data
# Download DrugBank xml dataset, drugbank_all_full_database.xml.zip
# to the data folder, requires registration

# Index with MongoDB  ~12m
./hmdb/drugbank.py --infile ./data/drugbank_all_full_database.xml.zip --db MongoDB

# Save as NetworkX graph
./hmdb/drugbank.py --infile ./data/drugbank_all_full_database.xml.zip --db NetworkX

```
