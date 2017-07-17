
## Index/Query scripts

* [index.py]() indexes MetaNetX compounds and reactions datasets
 together with their xref data

* [../tests/query-metanetx.py]() includes few sample queries
 for the indexed MetaNetX data


#### MetaNetX downloads page

http://www.metanetx.org/mnxdoc/mnxref.html

See [../tests/download.sh]() for example download command-line


#### Typical indexing times
* MongoDB: 20 mins
* Elasticsearch: 10 mins


#### TODO
* Use MongoDB Bulk Write API for more efficient indexing
