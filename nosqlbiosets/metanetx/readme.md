
## MetaNetX index/query scripts

* [index.py](index.py) index MetaNetX compounds, compartments, and reactions
 data including the xref data, tested with MetaNetX Aug 2019 release, version 3.2

* [query.py](query.py) query MetaNetX compounds, compartments, and reactions 

* [../../tests/test_metanetx_queries.py](../../tests/test_metanetx_queries.py)


## MetaNetX downloads

http://www.metanetx.org/mnxdoc/mnxref.html

```bash
# Download MetaNetX csv files
mkdir -p data/metanetx
wget -nc -P data/metanetx -r ftp://ftp.vital-it.ch/databases/metanetx/MNXref/latest/\
  --no-directories
```


## Example command lines

Index with Elasticsearch or MongoDB:  
```bash
# Index with Elasticsearch, requires about 10-15m
 ./nosqlbiosets/metanetx/index.py --metanetxdatafolder ./data/metanetx\
        --db Elasticsearch;
# Index with MongoDB, requires ~10m
 ./nosqlbiosets/metanetx/index.py --metanetxdatafolder ./data/metanetx\
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


#### Notes
MetaNetX scripts here use the words 'metabolite' and 'compound' with 
the same meaning:

A compound in chemistry is a combination of atoms from at least two
different elements. Compounds do not contain mixtures of elements.
They form through a chemical binding process and are in stable ratios.

Metabolites are the intermediates and products of metabolism.

The term metabolite is usually restricted to small molecules.
A small molecule is a low molecular weight (< 900 daltons) organic compound.
