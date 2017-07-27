
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
section of the [readme.md](../../readme.md) file on project main folder._

```bash
$ ./nosqlbiosets/uniprot/index.py --infile ../uniprot_sprot.xml.gz\
 --host localhost --db Elasticsearch

$ ./nosqlbiosets/uniprot/index.py --infile ../uniprot_sprot.xml.gz\
 --host localhost --db MongoDB 
```
