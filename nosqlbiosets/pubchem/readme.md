
> PubChem consists of three inter-linked databases, Substance, Compound and BioAssay.
The Substance database contains chemical information deposited by individual
data contributors to PubChem,
and the Compound database stores unique chemical structures
extracted from the Substance database.
Biological activity data of chemical substances tested in assay experiments
are contained in the BioAssay database.
[https://doi.org/10.1093/nar/gkv951]

### Index script for PubChem BioAssays json files 

* [`./index_bioassays.py`](index_bioassays.py) reads and indexes
compressed and archived PubChem BioAssay json files,
without extracting them to temporary files

  ```
  ./nosqlbiosets/pubchem/index_bioassays.py --help
  usage: index_bioassays.py [-h] [--infile INFILE] [--index INDEX] [--host HOST]
                              [--port PORT] [-db DB]
    
  Index PubChem Bioassays json files with Elasticsearch or MongoDB
    
  optional arguments:
      -h, --help            show this help message and exit
      --infile INFILE, --infolder INFILE
                            Input file to index, or input folder with zipped
                            bioassay json files
      --index INDEX         Name of Elasticsearch index or MongoDB database
      --host HOST           Elasticsearch/MongoDB server hostname
      --port PORT           Elasticsearch/MongoDB server port
      -db DB, --db DB       Database: 'Elasticsearch' or 'MongoDB'
  ```

  ```bash

  # Index with MongoDB
  # Zip file of json files
  ./nosqlbiosets/pubchem/index_bioassays.py --db MongoDB\
     --infile ./data/pubchem/bioassays/1259001_1260000.zip
  # Folder of zip files of json files
  ./nosqlbiosets/pubchem/index_bioassays.py --db MongoDB\
     --infolder ./data/pubchem/bioassays

  # Index with Elasticsearch
  # Zip file of json files
  ./nosqlbiosets/pubchem/index_bioassays.py --db Elasticsearch\
     --infile ./data/pubchem/bioassays/1259001_1260000.zip
 
  ```

#### Elasticsearch server settings
Since some of the PubChem BioAssay json files are large they require to change
few Elasticsearch default settings to higher values:

* Heap memory
    * _Elasticsearch-5_: Set `-Xms` AND `-Xmx` JVM settings to at least 14 GB,
    in configuration file `config/jvm.options`
    * _Elasticsearch-2_: Set `ES_MIN_MEM` AND `ES_MAX_MEM` environment variables
     to at least 1 and 14 GBs,
     (defaults are 256mb and 1GB), before calling your Elasticsearch server
    start script `bin/elasticsearch`
* Set `http.max_content_length: 800mb`, default 100mb,
  in your Elasticsearch configuration file `config/elasticsearch.yml`
* Large entries mean more garbage collection;
  [make sure garbage collection is fast](
https://www.elastic.co/guide/en/elasticsearch/reference/current/setup-configuration-memory.html) 
  by preventing any Elasticsearch memory from being swapped out 


#### TODO:
* Support for large bioassay json files,
  see `MaxEntrySize` in [index_bioassays.py](index_bioassays.py)
