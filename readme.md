# Scripts to index sample bioinformatics datasets with NoSQL databases 

We want to develop scripts for NoSQL indexing and querying of
sample bioinformatics datasets.
In the early stages of the project we developed scripts supporting
Elasticsearch only. In most recent scripts we have implemented
support for MongoDB as well.

Following are the set of draft scripts we have,
all supports Elasticsearch and few supports MongoDB.

* PubChem BioAssay json files: http://ftp.ncbi.nlm.nih.gov/pubchem/Bioassay
* WikiPathways gpml files: http://www.wikipathways.org/index.php/Download_Pathways
* PMC articles: http://ftp.ebi.ac.uk/pub/databases/pmc/manuscripts
* Ensembl regulatory build GFF files
  ([ensembl_regbuild.py](geneinfo/ensembl_regbuild.py))
  http://ftp.ensembl.org/pub/current_regulation/homo_sapiens
* HMDB protein/metabolite records:
  http://www.hmdb.ca/downloads
* NCBI PubTator gene2pub and disease2pub mappings:
  http://ftp.ncbi.nlm.nih.gov/pub/lu/PubTator
* MetanetX compound/reaction data files:
  http://www.metanetx.org/mnxdoc/mnxref.html
* HGNC, genenames.org data files: http://www.genenames.org/cgi-bin/statistics,
  http://ftp.ebi.ac.uk/pub/databases/genenames/new/json/
* RNAcentral identifier mappings
  ([rnacentral_idmappings.py](geneinfo/rnacentral_idmappings.py))
  http://ftp.ebi.ac.uk/pub/databases/RNAcentral/current_release/id_mapping/
* KBase compounds/reactions data files
  ([kbase/index.py](nosqlbiosets/kbase/index.py))
  http://ftp.kbase.us/assets/KBase_Reference_Data/Biochemistry/
* KEGG pathway kgml/xml files, pathway maps linked to gene ids
  ([kegg/index.py](nosqlbiosets/kegg/index.py))
  http://www.kegg.jp/kegg/download/Readme/README.kgml

We want to connect above datasets as much as possible
and aim to implement scripts with example queries for individual indexes
as well as connected data

In a separate [project](https://github.com/uludag/hspsdb-indexer)
we have developed index scripts for sequence
similarity search results, either in NCBI-BLAST xml/json formats
or in SAM/BAM formats

### Notes on PMC articles

[index-pmc-articles.py](index-pmc-articles.py) reads and indexes archives
of PMC articles xml files.

Install `pubmed_parser` library using its `setup.py` file or using `pip`
```
git clone https://github.com/titipata/pubmed_parser.git
python setup.py install --user
```
```
pip install --upgrade --user git+https://github.com/titipata/pubmed_parser.git
```
### Notes on PubTator datasets 

[index-pubtator-files.py](pubtator/index-pubtator-files.py)
reads and indexes NCBI PubTator gene2pub and disease2pub mappings

TODO:
* Support for other PubTator mappings
* Gene-disease network? (neo4j?, networkx?)

### Notes on PubChem datasets

[index-pubchem-bioassays.py](index-pubchem-bioassays.py) reads and indexes
the compressed and archived PubChem BioAssay json files,
without extracting them to temporary files

TODO:
* Support for large entries, such as larger than 800MB
* Use bulk index API
* Index other PubChem data types

### Notes on WikiPathways datasets

[index-wikipathways.py](index-wikipathways.py) reads and indexes
the archived WikiPathways gpml files,
without extracting them to temporary files

### Elasticsearch server settings
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

### Datasets we are considering to include: 
* REACTOME?, Rhea?, IntEnz? BioCyc?

## Copyright
This project has been developed
at King Abdullah University of Science and Technology (http://www.kaust.edu.sa)

## Acknowledgement
Computers and file systems used in developing this work has been maintained by John Hanks
