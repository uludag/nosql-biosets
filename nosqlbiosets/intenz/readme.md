
# Index script for IntEnz Enzyme database

* [index.py](index.py): Index [IntEnz](http://www.ebi.ac.uk/intenz/) xml files,
  tested with IntEnz release Nov 2017

* [query.py](query.py): Query API, at its early stages,
  more with MongoDB, few with Neo4j

* [tests.py](tests.py): Tests for the query API

## Example command lines
_Server default connection settings are read from [../../conf/dbservers.json](
../../conf/dbservers.json
)_

```bash
# Download IntEnz xml files data
mkdir -p data
wget -P ./data http://ftp.ebi.ac.uk/pub/databases/intenz/xml/ASCII/intenz.xml

# Index with Elasticsearch, requires ~3m
./nosqlbiosets/intenz/index.py --db Elasticsearch --infile ./data/intenz.xml\
 --index intenz

# Index with MongoDB, requires ~2m
./nosqlbiosets/intenz/index.py --db MongoDB --infile ./data/intenz.xml

# Index with Neo4j (processing time ~ 12m)
./nosqlbiosets/intenz/index.py --db Neo4j --infile ./data/intenz.xml

```
