
## Index/query scripts for HMDB and DrugBank xml datasets

* [index.py](index.py) Index HMDB protein and metabolite datasets.
  Tests made with HMDB version 4.0; _metabolites_ July 2018 update,
  _proteins_ July 2018 update

* [../tests/test_hmdb_queries.py](../tests/test_hmdb_queries.py)
  Includes example queries

* [drugbank.py](drugbank.py) Index DrugBank xml dataset with MongoDB,
  or Elasticsearch, or save drug-drug interactions as graph file in GML format.
  Tests made with DrugBank version 5.1, July 2018 update

* [queries.py](queries.py) Query API for DrugBank data indexed with MongoDB,
  _at its early stages_
  
  When executed from command line can save DrugBank
  interaction networks as graph files in four different formats,
  _example command lines are presented later on this page_ 
   * `--qc`: MongoDB query clause to select subsets of DrugBank entries
   * `--connections`: Connection type: "targets", "enzymes", "transporters" or
    "carriers"
   * `--graphfile`: File name for saving the output graph;
    If the file name ends with .xml extension [GraphML](
    https://en.wikipedia.org/wiki/GraphML) format is selected,
    if the file name ends with .d3.json extension graph is saved in
    a form easier to read with [D3js](://d3js.org),
    if the file name ends with .json extension graph is saved in
    [Cytoscape.js](://js.cytoscape.org) graph format,
    otherwise it is saved in GML format

### Index HMDB

```bash
# Download metabolites and proteins data
mkdir -p data
wget -P ./data http://www.hmdb.ca/system/downloads/current/hmdb_metabolites.zip
wget -P ./data http://www.hmdb.ca/system/downloads/current/hmdb_proteins.zip

# Index with Elasticsearch, time for proteins is ~10m, for metabolites ~140m
./hmdb/index.py --infile ./data/hmdb_metabolites.zip --db Elasticsearch --index hmdb_metabolite
./hmdb/index.py --infile ./data/hmdb_proteins.zip --db Elasticsearch --index hmdb_protein

# Index with MongoDB, time for proteins is ~8m, for metabolites ~100m
./hmdb/index.py --infile ./data/hmdb_metabolites.zip --db MongoDB --index biosets
./hmdb/index.py --infile ./data/hmdb_proteins.zip --db MongoDB --index biosets
```


### Index DrugBank

Download DrugBank xml dataset from http://www.drugbank.ca/releases/latest,
requires registration. Save `drugbank_all_full_database.xml.zip` file to the
`data` folder

```bash
# Index with MongoDB,  takes ~20m, with MongoDB Atlas ~35m
./hmdb/drugbank.py --infile ./data/drugbank_all_full_database.xml.zip\
 --db MongoDB --index biosets

# Index with Elasticsearch,  takes ~20m
./hmdb/drugbank.py --infile ./data/drugbank_all_full_database.xml.zip\
 --db Elasticsearch --index drugbank

# Save drug-drug interactions as graph file in GML format
# takes ~15m,  #edges ~ 660 000, #nodes ~ 3140
./hmdb/drugbank.py --infile ./data/drugbank_all_full_database.xml.zip --db NetworkX

```

### DrugBank graph queries

Example command lines to generate and save graphs for subsets of DrugBank data
or for the complete set

```bash

# Complete drug-targets graph 
./hmdb/queries.py --qc='{}' --graphfile targets.xml

# Complete drug-enzymes graph
./hmdb/queries.py --qc='{}' --graphfile enzymes.xml --connections=enzymes

# Drug-carriers graph for drugs with "Serum albumin" carrier
./hmdb/queries.py --qc='{"carriers.name": "Serum albumin"}'\
 --graphfile carriers-sa.xml --connections carriers

# Drug-targets graph for drugs with "side effects"
./hmdb/queries.py --qc='{"$text": {"$search": "side effects"}}'\
 --graphfile carriers-sa.xml --connections targets


```

#### Example graph

* [../docs/example-graphs/defensin-targets.json](
../docs/example-graphs/defensin-targets.json)

* [../docs/example-graphs/drug-targets.html](
../docs/example-graphs/drug-targets.html)


## Related work

* [https://github.com/egonw/create-bridgedb-hmdb](),
  [http://www.bridgedb.org/]()
  BridgeDB identity mapping files from HMDB, ChEBI, and Wikidata 
