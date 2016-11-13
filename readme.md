# Scripts to index sample bioinformatics datasets with Elasticsearch 

Inspired by the [nosql-tests](https://github.com/weinberger/nosql-tests/)
project we want to develop scripts for NoSQL indexing and querying of
sample bioinformatics datasets.
We are in early stages and for now we only have two scripts which are
for indexing

* [PubChem BioAssay json files](ftp://ftp.ncbi.nlm.nih.gov/pubchem/Bioassay/JSON/)
* [WikiPathways gpml files](http://www.wikipathways.org/index.php/Download_Pathways)

### Notes on PubChem dataset 

[index-pubchem-bioassays.py](index-pubchem-bioassays.py) script reads
the zipped and compressed PubChem BioAssay json files,
without extracting them to temporary files, and indexes them.

TODO:

* Support for entries larger than 800mb
* Representation of date fields in native date formats, this would
  improve handling of large entries, as some large entries have
  more than 300k date values
 
### Notes on WikiPathways dataset 

[index-wikipathways.py](index-wikipathways.py) script reads
the zipped WikiPathhways gpml files,
without extracting them to temporary files, and indexes them.

### Elasticsearch server settings
Since some of the PubChem BioAssay json files are large we need to change
two Elasticsearch default settings to higher values:
todo: ES_JAVA_OPTS

* Set `ES_MIN_MEM` AND `ES_MAX_MEM` environment variables to at least 1 and 14 GBs,
  defaults are 256mb and 1GB, before calling your Elasticsearch server
  start script `bin\elasticsearch`
```Shell
  export ES_MIN_MEM=1g
  export ES_MAX_MEM=14g
```  
* Set `http.max_content_length: 800mb`, default 100mb,
  in your Elasticsearch configuration file `config/elasticsearch.yml`
* Large entries mean much garbage collection activity;
  [make sure garbage collection is fast](https://www.elastic.co/guide/en/elasticsearch/reference/current/setup-configuration-memory.html) 
  by preventing any Elasticsearch memory from being swapped out
  

### Datasets we are considering to include: 
* Biocyc?, REACTOME?, Rhea?
* Gene names and synonyms?
* Sample sequence similarity search results, in BLAST xml2 and SAM formats?
