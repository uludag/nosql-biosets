
## Index/query scripts for HMDB and DrugBank xml datasets

* [index.py](index.py) Index HMDB proteins and metabolites datasets.
  Tests made with HMDB version 4.0

* [../tests/test_hmdb_queries.py](../tests/test_hmdb_queries.py)
  Includes example queries

* [drugbank.py](drugbank.py) Index DrugBank xml dataset with MongoDB,
  or Elasticsearch, or save drug-drug interactions as graph file in GML format.
  Tests made with DrugBank version 5.0

* [queries.py](queries.py) Query API for DrugBank data indexed with MongoDB,
  _at its early stages_.
  
  When executed from command line can save DrugBank
   interaction networks as graph files in GML format, _an example command line
    is presented further on this page_ 
   * `--qc`: MongoDB query clause to select subsets of DrugBank entries
   *  `--graphfile`: File name for saving the output graph;
    If the file name ends with .xml extension [GraphML](
    https://en.wikipedia.org/wiki/GraphML) format is selected,
    if the file name ends with .d3.json extension graph is saved in
     a form easier to read with [D3js](d3js.org),
    if the file name ends with .json extension graph is saved in
     [Cytoscape.js](js.cytoscape.org) graph format,     
    otherwise it is saved in GML format
### Usage HMDB

```bash
# Download metabolites and proteins data
mkdir -p data
wget -P ./data http://www.hmdb.ca/system/downloads/current/hmdb_metabolites.zip
wget -P ./data http://www.hmdb.ca/system/downloads/current/hmdb_proteins.zip

# Index with Elasticsearch, time for proteins is ~7m, for metabolites ~33m
./hmdb/index.py --infile ./data/hmdb_metabolites.zip --db Elasticsearch
./hmdb/index.py --infile ./data/hmdb_proteins.zip --db Elasticsearch

# Index with MongoDB, time for proteins is ~5m, for metabolites ~20m
./hmdb/index.py --infile ./data/hmdb_metabolites.zip --db MongoDB
./hmdb/index.py --infile ./data/hmdb_proteins.zip --db MongoDB
```


### Usage DrugBank

Download DrugBank xml dataset from http://www.drugbank.ca/releases/latest,
requires registration. Save `drugbank_all_full_database.xml.zip` file to the
`data` folder

```bash
# Index with MongoDB,  takes ~10-15m
./hmdb/drugbank.py --infile ./data/drugbank_all_full_database.xml.zip --db MongoDB

# Index with Elasticsearch,  takes ~22m
./hmdb/drugbank.py --infile ./data/drugbank_all_full_database.xml.zip --db Elasticsearch

# Save drug-drug interactions as graph file in GML format
# takes ~10m,  #edges = 578072, #nodes = 2550
./hmdb/drugbank.py --infile ./data/drugbank_all_full_database.xml.zip --db NetworkX

```

#### DrugBank graphs

Example command line to generate and save graphs from subsets of DrugBank data

```bash
./hmdb/queries.py --qc='{"carriers.name": "Serum albumin"}' --graphfile targets-sa.gml
```

## Related work

* https://github.com/ashrafsarhan/drugbank-relational-database
  Project home page includes graph of the DrugBank schema which could be useful
  in understanding the DrugBank data. _The graph is for version 4.3 of the
  database_
