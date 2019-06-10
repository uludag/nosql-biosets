# Index script for ClinVar Variation Archive dataset

ClinVar is freely available, public archive of the relationships
between medically important variants and phenotypes​

https://www.ncbi.nlm.nih.gov/clinvar/

* ~510K VariationArchive entries
* Compressed xml file size ~0.5G, uncompressed ~7G 

```bash

wget -P ./data ftp://ftp.ncbi.nlm.nih.gov/pub/clinvar/xml/clinvar_variation/ClinVarVariationRelease_00-latest.xml.gz


# requires about 40min
./nosqlbiosets/variation/clinvar.py --dbtype MongoDB\
   ./data/ClinVarVariationRelease_00-latest.xml.gz

# requires about 100min
./nosqlbiosets/variation/clinvar.py --dbtype Elasticsearch\
   ./data/clinvar/ClinVarVariationRelease_2019-05.xml --esindex clinvarvariation

```
