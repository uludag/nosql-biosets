# Index script for ClinVar Variation Archive dataset

ClinVar is freely available, public archive of the relationships
between medically important variants and phenotypes​

https://www.ncbi.nlm.nih.gov/clinvar/

* ~510K VariationArchive entries
* Compressed xml file size ~0.5G, uncompressed ~7G 

_Tested with Sep 2019 release_

```bash

wget -P ./data ftp://ftp.ncbi.nlm.nih.gov/pub/clinvar/xml/clinvar_variation/ClinVarVariationRelease_00-latest.xml.gz


# example indexing times we have seen 40m, 60m, 66m
./nosqlbiosets/variation/clinvar.py --dbtype MongoDB\
   ./data/ClinVarVariationRelease_00-latest.xml.gz

# requires about 110min
./nosqlbiosets/variation/clinvar.py --dbtype Elasticsearch\
   ./data/ClinVarVariationRelease_00-latest.xml.gz --esindex clinvarvariation

```
