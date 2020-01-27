
# Index/query scripts for IntEnz enzyme dataset

* [index.py](index.py): Index [IntEnz](http://www.ebi.ac.uk/intenz) xml files,
  tested with IntEnz December 2019 release
  
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

* [query.py](query.py): Query API (naive and not comprehensive),
  more queries with MongoDB, few with Neo4j
  
  ```text
    $ ./nosqlbiosets/intenz/query.py --help
    usage: query.py [-h] [--limit LIMIT] qc outfile
    
    Save IntEnz reaction connections as graph files
    
    positional arguments:
      qc             MongoDB query clause to select subsets of IntEnz entries,
                     e.g.: '{"reactions.label.value": "Chemically balanced"}'
      outfile        File name for saving the output graph. Format is selected
                     based on the file extension of the given output file; .xml
                     for GraphML, .gml for GML, .json for Cytoscape.js, or
                     .d3js.json for d3js format
    
    optional arguments:
      -h, --help     show this help message and exit
      --limit LIMIT  Maximum number of enzyme-metabolite connections
  ```

  ```bash
  ./nosqlbiosets/intenz/query.py '{"reactions.label.value": "Chemically balanced"}'\
    balanced-reactions.xml --limit 800
  
  ./nosqlbiosets/intenz/query.py '{"cofactors.#text": "Pyrroloquinoline quinone"}'\
    cofactors.json
  
  ./nosqlbiosets/intenz/query.py '{"$text": {"$search": "poly(A)"}}' polyA.json
  ```

* [tests.py](test_queries.py): Tests with the query API

## Example graph

* [../../docs/example-graphs/cofactors.json](
../../docs/example-graphs/cofactors.json)

* [../../docs/example-graphs/enzyme-cofactors.html](
../../docs/example-graphs/enzyme-cofactors.html)


## Example command lines for indexing
_Server default connection settings are read from [../../conf/dbservers.json](
../../conf/dbservers.json
)_

```bash
# Download IntEnz xml files

wget -nc -P ./data http://ftp.ebi.ac.uk/pub/databases/intenz/xml/ASCII/intenz.xml

# Index with Elasticsearch, requires ~ 5m to 15m
./nosqlbiosets/intenz/index.py --db Elasticsearch --infile ./data/intenz.xml\
 --index intenz

# Index with MongoDB, requires ~1m with local server, ~12m with MongoDB Atlas
./nosqlbiosets/intenz/index.py --db MongoDB --infile ./data/intenz.xml

# Index with Neo4j (processing time ~ 12m)
./nosqlbiosets/intenz/index.py --db Neo4j --infile ./data/intenz.xml

```
