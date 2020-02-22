
## Index/query scripts for HMDB and DrugBank xml datasets

* [index.py](index.py) Index HMDB protein and metabolite datasets.
  Tests made with HMDB version 4.0; _metabolites_ Jan 2019 update,
  _proteins_ Jan 2019 update

* [../tests/test_hmdb_queries.py](../tests/test_hmdb_queries.py)
  Includes example queries

* [drugbank.py](drugbank.py) Index DrugBank xml dataset with MongoDB,
  or Elasticsearch, or save drug-drug interactions as graph file in GML format.
  Tests made with DrugBank version 5.1.5, January 2020 update
  
```bash
./hmdb/drugbank.py --help
usage: drugbank.py [-h] -infile INFILE [--index INDEX] [--doctype DOCTYPE]
                   [--host HOST] [--port PORT] [--db DB]
                   [--graphfile GRAPHFILE] [--allfields]

Index DrugBank entries in xml format, with MongoDB or Elasticsearch, downloaded from
https://www.drugbank.ca/releases/latest

optional arguments:
  -h, --help            show this help message and exit
  -infile INFILE, --infile INFILE
                        Input file name
  --index INDEX         Name of the MongoDB database or Elasticsearch index,
                        or filename for NetworkX graph
  --doctype DOCTYPE     MongoDB collection name or Elasticsearch document type
                        name
  --host HOST           MongoDB or Elasticsearch server hostname
  --port PORT           MongoDB or Elasticsearch server port number
  --db DB               Database: 'MongoDB' or 'Elasticsearch', if not set
                        drug-drug interaction network is saved to a graph file
                        specified with the '--graphfile' option
  --graphfile GRAPHFILE
                        Database: 'MongoDB' or 'Elasticsearch',or if
                        'graphfile' drug-drug interactionnetwork saved as
                        graph file
  --allfields           By default sequence fields and the patents field is
                        not indexed. Select this option to index all fields
```

* [queries.py](queries.py) Query API for DrugBank data indexed with MongoDB,
  _at its early stages_

```text
./hmdb/queries.py --help
usage: queries.py [-h] {savegraph,cyview} ...

positional arguments:
  {savegraph,cyview}
    savegraph         Save DrugBank interactions as graph files
    cyview            See HMDB/DrugBank graphs with Cytoscape runing on your local machine

./hmdb/queries.py savegraph --help
./hmdb/queries.py cyview --help
```

### Index HMDB

```bash
# Download metabolites and proteins data
mkdir -p data
wget -P ./data http://www.hmdb.ca/system/downloads/current/hmdb_metabolites.zip
wget -P ./data http://www.hmdb.ca/system/downloads/current/hmdb_proteins.zip

# Index with Elasticsearch, time for proteins is ~15m, for metabolites ~ 30m to 250m
./hmdb/index.py --infile ./data/hmdb_metabolites.zip --db Elasticsearch --index hmdb_metabolite
./hmdb/index.py --infile ./data/hmdb_proteins.zip --db Elasticsearch --index hmdb_protein

# Index with MongoDB, time for proteins is ~ 2m to 8m, for metabolites ~ 20m to 100m
./hmdb/index.py --infile ./data/hmdb_metabolites.zip --db MongoDB --index biosets
./hmdb/index.py --infile ./data/hmdb_proteins.zip --db MongoDB --index biosets

# Index with project's main index script
./scripts/nosqlbiosets index hmdb MongoDB ~/data/hmdb/hmdb_proteins.zip
./scripts/nosqlbiosets index hmdb MongoDB ~/data/hmdb/hmdb_metabolites.zip

./scripts/nosqlbiosets index hmdb Elasticsearch ~/data/hmdb/hmdb_proteins.zip --index hmdb_protein

```


### Index DrugBank

Download DrugBank xml dataset from http://www.drugbank.ca/releases/latest,
requires registration. Save `drugbank_all_full_database.xml.zip` file to the
`data` folder

```bash
# Index with MongoDB,  takes ~ 5m to 30m, with MongoDB Atlas ~50m?
./hmdb/drugbank.py --infile ./data/drugbank_all_full_database.xml.zip\
 --db MongoDB --index biosets

./scripts/nosqlbiosets index drugbank MongoDB ~/data/drugbank/drugbank-5.1.2.xml.zip

# Index with Elasticsearch,  takes ~ 20 to 50m
./hmdb/drugbank.py --infile ./data/drugbank_all_full_database.xml.zip\
 --db Elasticsearch --index drugbank

# Save drug-drug interactions as graph file in GML format
# (not a mature feature: queries.py have better response time
#                        and is the preferred way for building interaction graphs)
# takes ~ 4m to 15m,  #edges ~ 2,712000, #nodes ~ 3950
./hmdb/drugbank.py --infile ./data/drugbank_all_full_database.xml.zip --db NetworkX

```

### DrugBank graph queries

Example command lines to generate and save graphs for subsets of DrugBank data
or for the complete set

```bash

# Complete drug-targets graph 
./hmdb/queries.py savegraph '{}' targets.xml

# Complete drug-enzymes graph
./hmdb/queries.py savegraph '{}' enzymes.xml --connections=enzymes

# Drug-carriers graph for drugs that have referencs to "Serum albumin"
./hmdb/queries.py savegraph '{"carriers.name": "Serum albumin"}'\
     carriers-sa.xml --connections carriers

# Drug-targets graph for drugs which have keyword "antitubercular" in text fields 
./hmdb/queries.py savegraph '{"$text": {"$search": "antitubercular"}}'\
     antitubercular.xml --connections targets

```

Example command lines to view graph results with Cytoscape

```bash
  ./hmdb/queries.py cyview --help
  ./hmdb/queries.py cyview --dataset HMDB meningitis
  ./hmdb/queries.py cyview --dataset drugbank meningitis
```

#### Example graphs

* [Drugs which have mention of term "methicillin",
 with their targets, enzymes, transportes, and carriers](
../docs/example-graphs/drugbank-methicillin.html)

* [Drugs with their targets, which have mention of term "defensin"](
../docs/example-graphs/drug-targets.html)


## Related work

* [https://github.com/egonw/create-bridgedb-hmdb](),
  [http://www.bridgedb.org/]()
  BridgeDB identity mapping files from HMDB, ChEBI, and Wikidata 

## Related links

* http://www.hmdb.ca/sources: a brief introduction to HMDB,
  and a detailed list of data sources for the data fields of HMDB entries
