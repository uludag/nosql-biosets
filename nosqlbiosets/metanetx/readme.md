
## Index/Query scripts

* [index.py](index.py) index MetaNetX compounds, compartments, and reactions
 data including the xref data.
  
  Sample command lines for indexing with Elasticsearch and MongoDB:
```bash
$ ./metanetx/index.py --metanetxdatafolder ./metanetx/data\
        --index metanetx --db Elasticsearch;
$ ./metanetx/index.py --metanetxdatafolder ./metanetx/data\
        --index metanetx --db MongoDB;
```

* [../../tests/query-metanetx.py](../../tests/query-metanetx.py)
 includes few sample queries for the indexed data


#### MetaNetX downloads page

http://www.metanetx.org/mnxdoc/mnxref.html

See [../../tests/download.sh](../../tests/download.sh)
 for example download command-line


#### Typical indexing times
* MongoDB: ~20 mins
* Elasticsearch: 10-15 mins


#### TODO
* Use MongoDB Bulk Write API for more efficient indexing
