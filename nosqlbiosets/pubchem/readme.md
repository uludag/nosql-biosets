### Index script for PubChem BioAssay data in json format 

* [`./index_bioassays.py`](index_bioassays.py) reads and indexes
compressed and archived PubChem BioAssay json files,
without extracting them to temporary files

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
* Support for large entries, such as larger than 800M
* Use bulk index API
* Index other PubChem data types
