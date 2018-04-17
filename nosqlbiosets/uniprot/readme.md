
# Index/query scripts for UniProtKB datasets

* [index.py](index.py): Index UniProtKB xml files
  
  _Tested with Swiss-Prot dataset only (March 2018 release)_
  
    ```
    ./nosqlbiosets/uniprot/index.py --help
    usage: index.py [-h] [--index INDEX] [--doctype DOCTYPE] [--host HOST]
                    [--port PORT] [--db DB]
                    infile
    
    Index UniProt xml files, with Elasticsearch or MongoDB
    
    positional arguments:
      infile             Input file name for UniProt Swiss-Prot compressedxml
                         dataset
    
    optional arguments:
      -h, --help         show this help message and exit
      --index INDEX      Name of the Elasticsearch index or MongoDB database
      --doctype DOCTYPE  Document type name for Elasticsearch, collection name for
                         MongoDB
      --host HOST        Elasticsearch or MongoDB server hostname
      --port PORT        Elasticsearch or MongoDB server port number
      --db DB            Database: 'Elasticsearch' or 'MongoDB'
    ```

* [query.py](query.py): Query API, at its early stages of development

* [../../tests/test_uniprot_queries.py](../../tests/test_uniprot_queries.py):
 Tests for the query API

                                      
## Usage

Example command lines for downloading `uniprot_sprot.xml` file and for indexing:

#### Download UniProt/Swiss-Prot data set

```bash
mkdir -p data
# ~700M
wget -P ./data ftp://ftp.ebi.ac.uk/pub/databases/uniprot/current_release/\
knowledgebase/complete/uniprot_sprot.xml.gz
```

#### Index with Elasticsearch or MongoDB
_If you have not already installed nosqlbiosets project see the Installation
section of the [readme.md](../../readme.md) file on project main folder._

_Server default connection settings are read from [../../conf/dbservers.json](
../../conf/dbservers.json
)_

```bash
# Index with Elasticsearch,  typically requires 7 to 8 hours
./nosqlbiosets/uniprot/index.py ./data/uniprot_sprot.xml.gz\
 --host localhost --db Elasticsearch  --index uniprot

# Index with MongoDB, typically requires 4 to 5 hours
./nosqlbiosets/uniprot/index.py ./data/uniprot_sprot.xml.gz\
 --host localhost --db MongoDB --index biosets
```

## PSI MI-TAB support

This folder also includes an index script for PSI-MI TAB protein interactions
data files

* [index_mitab.py](index_mitab.py) Index PSI-MI TAB data files
 with Elasticsearch or MongoDB
  * _At its early stages, field names were selected similart to
   the filed names in [Molecular Interactions Query Language](
   http://psicquic.github.io/MiqlReference27.html)_
  * _Tested with [HIPPIE](http://cbdm-01.zdv.uni-mainz.de/~mschaefer/hippie)
   database only, Human Integrated Protein-Protein Interaction rEference_ 
 
 ### Links for the PSI MI-TAB format
 - https://wiki.thebiogrid.org/doku.php/psi_mitab_file
 - http://psicquic.github.io/MITAB27Format.html
 - https://wiki.reactome.org/index.php/PSI-MITAB_interactions
 
```bash

wget -P ./data http://cbdm-01.zdv.uni-mainz.de/~mschaefer/hippie/HIPPIE-current.mitab.txt

# Index with Elasticsearch
./nosqlbiosets/uniprot/index_mitab.py --infile ./data/HIPPIE-current.mitab.txt\
 --db Elasticsearch

# Index with MongoDB
./nosqlbiosets/uniprot/index_mitab.py --infile ./data/HIPPIE-current.mitab.txt\
 --db MongoDB
```
 HIPPIE indexing takes ~8m with MongoDB, ~2m with Elasticsearch,
  _we will move to MongoDB bulk API to improve MongoDB indexing speed_
