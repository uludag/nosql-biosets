
## MetaNetX index/query scripts

* [index.py](index.py) index MetaNetX compounds, compartments, and reactions
 data including the xref data, tested with MetaNetX Oct 2018 release, version 3.1

* [query.py](query.py) query MetaNetX compounds, compartments, and reactions 

* [../../tests/test_metanetx_queries.py](../../tests/test_metanetx_queries.py)

Command lines for indexing with Elasticsearch and MongoDB:
  
```bash
 ./nosqlbiosets/metanetx/index.py --metanetxdatafolder /local/data/metanetx\
        --db Elasticsearch;

 ./nosqlbiosets/metanetx/index.py --metanetxdatafolder /local/data/metanetx\
        --index biosets --db MongoDB;
```


Elasticsearch query to get distribution of reactions among source/reference
libraries: 
 ```bash
curl -XGET "http://localhost:9200/metanetx_reaction/_search?pretty=true"\
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

#### MetaNetX downloads

http://www.metanetx.org/mnxdoc/mnxref.html

```bash
mkdir -p data/metanetx
cd data/metanetx
wget -r ftp://ftp.vital-it.ch/databases/metanetx/MNXref/latest/ --no-directories
```

#### Typical indexing times
* MongoDB: 6-8 min 
* Elasticsearch: 10 min

#### Notes
MetaNetX scripts here use the words 'metabolite' and 'compound' with 
the same meaning:

A compound in chemistry is a combination of atoms from at least two
different elements. Compounds do not contain mixtures of elements.
They form through a chemical binding process and are in stable ratios.

Metabolites are the intermediates and products of metabolism.

The term metabolite is usually restricted to small molecules.
A small molecule is a low molecular weight (< 900 daltons) organic compound.
