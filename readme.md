# Project aim 

We want to develop scripts for NoSQL indexing and querying of sample
bioinformatics datasets.

In the early stages of the project only Elasticsearch was supported.
In most recent work (UniProt, MetaNetX, HMDB, SBML files)
we have implemented MongoDB support as well.

## Datasets supported

* MetaNetX [compounds, reactions, and compartments data](
http://www.metanetx.org/mnxdoc/mnxref.html
): [`./metanetx`](./metanetx)
  

* Metabolic network files in [SBML](http://sbml.org) or
 [PSAMM yaml](https://github.com/zhanglab/psamm-model-collection) formats:
  [`./nosqlbiosets/pathways/index_metabolic_networks.py`](
  nosqlbiosets/pathways/index_metabolic_networks.py)
   (_recent work, not tested with many network files_)

* UniProtKB [datasets](
ftp://ftp.ebi.ac.uk/pub/databases/uniprot/current_release/knowledgebase/complete/
) in XML format:
  [`./nosqlbiosets/uniprot`](nosqlbiosets/uniprot)
  
* PubChem [BioAssay](http://ftp.ncbi.nlm.nih.gov/pubchem/Bioassay) json files:
  [`./nosqlbiosets/pubchem`](
  nosqlbiosets/pubchem)  

* WikiPathways [gpml files](
http://www.wikipathways.org/index.php/Download_Pathways):
  [`./nosqlbiosets/pathways/index_wikipathways.py`](
  ./nosqlbiosets/pathways/index_wikipathways.py)

* PMC [articles](http://ftp.ebi.ac.uk/pub/databases/pmc/manuscripts):
  [`./index-pmc-articles.py`](index-pmc-articles.py)

* Ensembl regulatory build [GFF files](
http://ftp.ensembl.org/pub/current_regulation/homo_sapiens):
  [`./geneinfo/ensembl_regbuild.py`]([geneinfo/ensembl_regbuild.py)    

* HMDB [proteins, metabolites datasets](http://www.hmdb.ca/downloads):
  [`./hmdb/index.py`](hmdb/index.py)

* NCBI PubTator [gene2pub and disease2pub mappings](
http://ftp.ncbi.nlm.nih.gov/pub/lu/PubTator):
  [`./nosqlbiosets/pubtator`](nosqlbiosets/pubtator)

* HGNC, [genenames.org](http://www.genenames.org/cgi-bin/statistics),
 [data files in json format](
 http://ftp.ebi.ac.uk/pub/databases/genenames/new/json/),
  from EBI: [`./geneinfo/index-hgnc-geneinfo.py`](geneinfo/index-hgnc-geneinfo)

* RNAcentral [identifier mappings](
http://ftp.ebi.ac.uk/pub/databases/RNAcentral/current_release/id_mapping/),
  [`./geneinfo/rnacentral_idmappings.py`](geneinfo/rnacentral_idmappings.py)

* KBase [compounds/reactions data files](
http://ftp.kbase.us/assets/KBase_Reference_Data/Biochemistry/):
  [`./nosqlbiosets/kbase/index.py`](nosqlbiosets/kbase/index.py)

* KEGG [pathway kgml/xml files](
http://www.kegg.jp/kegg/download/Readme/README.kgml):
  [`./nosqlbiosets/kegg/index.py`](nosqlbiosets/kegg/index.py)
  (_KEGG data distribution policy lets us think twice when spending
   time on KEGG data_)
  

We want to connect above datasets as much as possible
and aim to implement scripts with example queries for individual indexes
as well as connected data. We want to implement automated tests as early as
possible, this should help us to understand where we are in minimal time. 

In a separate [project](https://github.com/uludag/hspsdb-indexer)
we have developed index scripts for sequence
similarity search results, either in NCBI-BLAST xml/json formats
or in SAM/BAM formats

## Installation

Download nosqlbiosets project source code and install required libraries:
```bash
$ git clone https://bitbucket.org/hspsdb/nosql-biosets.git
$ cd nosql-biosets
$ pip install -r requirements.txt --user
```

Since we are yet in early stages you may need to check (and modify)
source code of the scripts time to time, for this reason light install
nosqlbiosets project to your local Python library/package folders
using the `setup.py` `develop` and `--user` options
that should allow you to run the index scripts from project
source folders:
```bash
$ python setup.py develop --user
```

Default values of hostname and port numbers of Elasticsearch and MongoDB servers
are read from [`./conf/dbservers.json`](conf/dbservers.json) file.
Save your settings in this file to avoid entering `--host` and `--port`
parameters in command line.

## Usage

Example command lines for downloading UniProt xml data files and for indexing:
```bash
$ wget ftp://ftp.ebi.ac.uk/pub/databases/uniprot/current_release/\
knowledgebase/complete/uniprot_sprot.xml.gz
```
Make sure your Elasticsearch server is running in your localhost.
If you are using Linux the easiest way is to [download Elasticsearch](
https://www.elastic.co/downloads/elasticsearch) with the TAR option (32M).
After extracting the tar file just `cd` to your Elasticsearch folder
and run `./bin/elasticsearch` command.

Now you can install your UniProt xml file by running the following command
from nosqlbiosets project root folder.  
```bash
$ ./nosqlbiosets/uniprot/index.py --infile ../uniprot_sprot.xml.gz\
 --host localhost --db Elasticsearch
```
Query top mentioned gene names: 
```bash
curl -XGET "http://localhost:9200/uniprot/_search?pretty=true"\
 -H 'Content-Type: application/json' -d'
{
  "size": 0,
  "aggs": {
    "genes": {
      "terms": {
        "field": "gene.name.#text.keyword",
        "size": 5
      },
      "aggs": {
        "tids": {
          "terms": {
            "field": "gene.name.type.keyword",
            "size": 5
          }
        }
      }
    }
  }
}'
```
Check [`./tests/query-uniprot.py`](tests/query-uniprot.py) for simple
example queries with Elasticsearch and MongoDB.

## Notes

### PMC articles

[index-pmc-articles.py](index-pmc-articles.py) reads and indexes archives
of PMC articles xml files.

Requires [`pubmed_parser`](https://github.com/titipata/pubmed_parser/)
library installed

### WikiPathways

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

### Datasets we are considering to include
* REACTOME?, Rhea?, IntEnz?, BioCyc?, [ConsensusPathDB](http://cpdb.molgen.mpg.de/)?

## Copyright
This project has been developed
at King Abdullah University of Science and Technology, http://www.kaust.edu.sa

## Acknowledgement
Computers and file systems used in developing this work has been maintained by John Hanks
