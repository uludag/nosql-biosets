# Index script for ClinVar Variation Archive dataset

ClinVar is freely available, public archive of the relationships
between medically important variants and phenotypes​

https://www.ncbi.nlm.nih.gov/clinvar/

* ~566K VariationArchive entries
* Compressed xml file size ~0.6G, uncompressed ~8G

_Tested with Oct 2019 release_

```bash

wget -P ./data ftp://ftp.ncbi.nlm.nih.gov/pub/clinvar/xml/clinvar_variation/ClinVarVariationRelease_00-latest.xml.gz


# example indexing times we have seen 40m, 60m, 3h:20m
./nosqlbiosets/variation/clinvar.py --dbtype MongoDB\
   ./data/ClinVarVariationRelease_00-latest.xml.gz

# example indexing times we have seen 110min, 5h:06m
./nosqlbiosets/variation/clinvar.py --dbtype Elasticsearch\
   ./data/ClinVarVariationRelease_00-latest.xml.gz --esindex clinvarvariation

```

## TODO
- All numeric types should be represented as numbers
