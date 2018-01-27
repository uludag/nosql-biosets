
## Index/query scripts for HMDB and DrugBank xml datasets

* [index.py](index.py) Index HMDB protein and metabolite datasets.
  Tests made with HMDB version 4.0; metabolites Dec 2017 update,
  proteins Jan 2018 update

* [../tests/test_hmdb_queries.py](../tests/test_hmdb_queries.py)
  Includes example queries

* [drugbank.py](drugbank.py) Index DrugBank xml dataset with MongoDB,
  or Elasticsearch, or save drug-drug interactions as graph file in GML format.
  Tests made with DrugBank version 5.0, Dec 2017 update

* [queries.py](queries.py) Query API for DrugBank data indexed with MongoDB,
  _at its early stages_
  
  When executed from command line can save DrugBank
   interaction networks as graph files in GML format, _an example command line
    is presented later on this page_ 
   * `--qc`: MongoDB query clause to select subsets of DrugBank entries
   *  `--graphfile`: File name for saving the output graph;
    If the file name ends with .xml extension [GraphML](
    https://en.wikipedia.org/wiki/GraphML) format is selected,
    if the file name ends with .d3.json extension graph is saved in
    a form easier to read with [D3js](://d3js.org),
    if the file name ends with .json extension graph is saved in
    [Cytoscape.js](://js.cytoscape.org) graph format,     
    otherwise it is saved in GML format

### Usage HMDB

```bash
# Download metabolites and proteins data
mkdir -p data
wget -P ./data http://www.hmdb.ca/system/downloads/current/hmdb_metabolites.zip
wget -P ./data http://www.hmdb.ca/system/downloads/current/hmdb_proteins.zip

# Index with Elasticsearch, time for proteins is ~10m, for metabolites ~60m
./hmdb/index.py --infile ./data/hmdb_metabolites.zip --db Elasticsearch --index hmdb_metabolites
./hmdb/index.py --infile ./data/hmdb_proteins.zip --db Elasticsearch --index hmdb_proteins

# Index with MongoDB, time for proteins is ~8m, for metabolites ~40m
./hmdb/index.py --infile ./data/hmdb_metabolites.zip --db MongoDB --index biosets
./hmdb/index.py --infile ./data/hmdb_proteins.zip --db MongoDB --index biosets
```


### Usage DrugBank

Download DrugBank xml dataset from http://www.drugbank.ca/releases/latest,
requires registration. Save `drugbank_all_full_database.xml.zip` file to the
`data` folder

```bash
# Index with MongoDB,  takes ~10-16m, with MongoDB Atlas ~35m
./hmdb/drugbank.py --infile ./data/drugbank_all_full_database.xml.zip --db MongoDB

# Index with Elasticsearch,  takes ~16-22m
./hmdb/drugbank.py --infile ./data/drugbank_all_full_database.xml.zip --db Elasticsearch

# Save drug-drug interactions as graph file in GML format
# takes ~15m,  #edges ~ 660 000, #nodes ~ 3140
./hmdb/drugbank.py --infile ./data/drugbank_all_full_database.xml.zip --db NetworkX

```

#### DrugBank graphs

Example command line to generate and save graphs from subsets of DrugBank data

```bash
./hmdb/queries.py --qc='{}' --graphfile targets.xml
./hmdb/queries.py --qc='{"carriers.name": "Serum albumin"}' --graphfile targets-sa.gml
```

## Related work

* https://github.com/egonw/create-bridgedb-hmdb, http://www.bridgedb.org/
  BridgeDB identity mapping files from HMDB, ChEBI, and Wikidata 

* https://github.com/ashrafsarhan/drugbank-relational-database
  Project home page includes graph of the DrugBank schema which could be useful
  in understanding the structure of DrugBank data.
  _The schema graph is for version 4.3 of the database_

* smpdb.ca indexer???