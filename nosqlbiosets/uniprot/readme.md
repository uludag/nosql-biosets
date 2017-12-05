
# Index/query scripts for UniProtKB datasets

* [index.py](index.py): Index UniProtKB xml files
  
  _Tested with Swiss-Prot dataset only (release Nov 2017),
  should also work with UniProtKB/TrEMBL dataset_

* [query.py](query.py): Experimental query API, at its early stages

* [../../tests/test_uniprot_queries.py](../../tests/test_uniprot_queries.py):
 Tests for the query API

                                      
## Usage

Example command lines for downloading `uniprot_sprot.xml` file and for indexing:

#### Download UniProt/Swiss-Prot data set

```bash
mkdir -p data
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
# Index with Elasticsearch
./nosqlbiosets/uniprot/index.py --infile ./data/uniprot_sprot.xml.gz\
 --host localhost --db Elasticsearch  --index uniprot

# Index with MongoDB
./nosqlbiosets/uniprot/index.py --infile ./data/uniprot_sprot.xml.gz\
 --host localhost --db MongoDB --index biosets
```

## PSI MI-TAB support

This folder also includes support for PSI-MI TAB interaction data files

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
mkdir -p data
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
