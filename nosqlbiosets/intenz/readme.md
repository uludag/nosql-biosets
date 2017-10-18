
# Index script for IntEnz Enzyme database

* [index.py](index.py): Index [IntEnz](http://www.ebi.ac.uk/intenz/) xml files

* [query.py](query.py): Experimental query API, at its early stages

* [tests.py](tests.py): Tests for the query API

## Example command lines
_Server default connection settings are read from [../../conf/dbservers.json](
../../conf/dbservers.json
)_

```bash
# Download IntEnz xml files data
mkdir -p data
wget -P ./data http://ftp.ebi.ac.uk/pub/databases/intenz/xml/ASCII/intenz.xml

# Index with Elasticsearch
./nosqlbiosets/intenz/index.py --db Elasticsearch --infile ./data/intenz.xml

# Index with MongoDB
./nosqlbiosets/intenz/index.py --db MongoDB --infile ./data/intenz.xml

# Index with Neo4j (processing time ~ 10-15m)
./nosqlbiosets/intenz/index.py --db Neo4j --infile ./data/intenz.xml

```
