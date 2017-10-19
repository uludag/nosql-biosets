
## MetaNetX index/query scripts

* [index.py](index.py) index MetaNetX compounds, compartments, and reactions
 data including the xref data

* [query.py](query.py) query MetaNetX compounds, compartments, and reactions 

* [../../tests/test_metanetx_queries.py](../../tests/test_metanetx_queries.py)

Command lines for indexing with Elasticsearch and MongoDB:
  
```bash
 ./metanetx/index.py --metanetxdatafolder ./metanetx/data\
        --index metanetx --db Elasticsearch;

 ./metanetx/index.py --metanetxdatafolder ./metanetx/data\
        --index metanetx --db MongoDB;
```


Elasticsearch query to get distribution of reactions among source/reference
libraries: 
 ```bash
curl -XGET "http://localhost:9200/biosets/metanetx_reaction/_search?pretty=true"\
 -H 'Content-Type: application/json' -d'
{
  "size": 0,
  "aggs": {
    "libs": {
      "terms": {
        "field": "xrefs.lib.keyword",
        "size": 10
      }
    }
  }
}'
```

#### MetaNetX downloads page

http://www.metanetx.org/mnxdoc/mnxref.html

See [../../tests/download.sh](../../tests/download.sh)
 for example download command-line


#### Typical indexing times
* MongoDB: ~20 mins
* Elasticsearch: 10-15 mins


#### TODO
* Use MongoDB Bulk Write API for more efficient indexing

#### Notes
MetaNetX scripts here used the words 'metabolite' and 'compound' with 
the same meaning:

A compound in chemistry is a combination of atoms from at least two
different elements. Compounds do not contain mixtures of elements.
They form through a chemical binding process and are in stable ratios.

Metabolites are the intermediates and products of metabolism.

The term metabolite is usually restricted to small molecules.
A small molecule is a low molecular weight (< 900 daltons) organic compound.
