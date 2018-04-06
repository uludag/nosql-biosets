
# Index/query scripts for IntEnz enzyme dataset

* [index.py](index.py): Index [IntEnz](http://www.ebi.ac.uk/intenz) xml files,
  tested with IntEnz March 2018 release
  
  ```text
    $ ./nosqlbiosets/intenz/index.py --help
    usage: index.py [-h] [-infile INFILE] [--index INDEX] [--doctype DOCTYPE]
                    [--host HOST] [--port PORT] [--db DB]
    
    Index IntEnz xml files, with Elasticsearch, MongoDB or Neo4j
    
    optional arguments:
      -h, --help            show this help message and exit
      -infile INFILE, --infile INFILE
                            Input file name (intenz/ASCII/intenz.xml)
      --index INDEX         Name of the Elasticsearch index or MongoDB database
      --doctype DOCTYPE     Document type name for Elasticsearch, collection name
                            for MongoDB
      --host HOST           Elasticsearch, MongoDB or Neo4j server hostname
      --port PORT           Elasticsearch, MongoDB or Neo4j server port
      --db DB               Database: 'Elasticsearch', 'MongoDB' or 'Neo4j'
  ```

* [query.py](query.py): Query API at its early stages,
  more queries with MongoDB, few with Neo4j
  
  ```text
    $ ./nosqlbiosets/intenz/query.py --help
    usage: query.py [-h] [--limit LIMIT] qc outfile
    
    Save IntEnz reaction connections as graph files
    
    positional arguments:
      qc             MongoDB query clause to select subsets of IntEnz entries, ex:
                     '{"reactions.label.value": "Chemically balanced"}'
      outfile        File name for saving the output graph in GraphML, GML,
                     Cytoscape.js or d3js formats, see readme.md for details
    
    optional arguments:
      -h, --help     show this help message and exit
      --limit LIMIT  Maximum number of reactant-product connections
  ```

  ```bash
  ./nosqlbiosets/intenz/query.py '{"reactions.label.value": "Chemically balanced"}'\
    docs/intenz-test.json --limit 80
  ```

* [tests.py](tests.py): Tests with the query API

## Example command lines
_Server default connection settings are read from [../../conf/dbservers.json](
../../conf/dbservers.json
)_

```bash
# Download IntEnz xml files data

wget -P ./data http://ftp.ebi.ac.uk/pub/databases/intenz/xml/ASCII/intenz.xml

# Index with Elasticsearch, requires ~3m
./nosqlbiosets/intenz/index.py --db Elasticsearch --infile ./data/intenz.xml\
 --index intenz

# Index with MongoDB, requires ~2m with local server, ~14m with MongoDB Atlas
./nosqlbiosets/intenz/index.py --db MongoDB --infile ./data/intenz.xml

# Index with Neo4j (processing time ~ 12m)
./nosqlbiosets/intenz/index.py --db Neo4j --infile ./data/intenz.xml

```
