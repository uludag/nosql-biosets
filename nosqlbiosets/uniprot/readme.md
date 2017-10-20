
# Index/query script for UniProtKB dataset

* [index.py](index.py): Index UniProtKB xml files (_tested with Swiss-Prot
 dataset, should also work with UniProtKB/TrEMBL dataset_)

* [query.py](query.py): Experimental query API, at its early stages

* [../../tests/test_uniprot_queries.py](../../tests/test_uniprot_queries.py):
 Tests for the query API

                                      
## Usage

Example command lines for downloading xml data files and for indexing:

#### Download UniProt/Swiss-Prot data set

```bash
$ wget ftp://ftp.ebi.ac.uk/pub/databases/uniprot/current_release/\
knowledgebase/complete/uniprot_sprot.xml.gz
```

#### Index with Elasticsearch or MongoDB
_If you have not already installed nosqlbiosets project see the Installation
section of the [readme.md](../../readme.md) file on project main folder._

_Server default connection settings are read from [../../conf/dbservers.json](
../../conf/dbservers.json
)_

```bash
$ ./nosqlbiosets/uniprot/index.py --infile ../uniprot_sprot.xml.gz\
 --host localhost --db Elasticsearch

$ ./nosqlbiosets/uniprot/index.py --infile ../uniprot_sprot.xml.gz\
 --host localhost --db MongoDB 
```
