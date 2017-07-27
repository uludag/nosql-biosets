
Index script for UniProtKB data sets in XML format.

Tested with Swiss-Prot dataset, should also work with UniProtKB/TrEMBL dataset.
                                      
## Usage
Example command lines for downloading UniProt xml files and indexing.



#### Download UniProt/Swiss-Prot data set

```bash
$ wget ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/\
knowledgebase/complete/uniprot_sprot.xml.gz
```

#### Index with Elasticasearch and/or MongoDB
_If you have not already installed nosqlbiosets project see the Installation
section further in this page._

```bash
$ ./nosqlbiosets/uniprot/index.py --infile ../uniprot_sprot.xml.gz\
 --host localhost --db Elasticsearch

$ ./nosqlbiosets/uniprot/index.py --infile ../uniprot_sprot.xml.gz\
 --host localhost --db MongoDB 
```

#### Installation

Download nosqlbiosets project source code and install required libraries:
```bash
$ git clone https://github.com/uludag/nosql-biosets.git
$ cd nosql-biosets
$ pip install -r requirements.txt --user
```

Install project to your local Python library/package folders or just add
current-working-directory(`.`) to your `PYTHONPATH` that should allow you
to run the index scripts from nosqlbiosets project source root folder:
```bash
$ python setup.py install --user
$ export PYTHONPATH=.:${PYTHONPATH}
```
