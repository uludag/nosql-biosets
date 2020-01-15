# Index/query scripts for ClinVar Variation Archive dataset

ClinVar is freely available, public archive of the relationships
between medically important variants and phenotypes​

https://www.ncbi.nlm.nih.gov/clinvar/

* ~676K VariationArchive entries
* Compressed xml file size ~0.7G, uncompressed ~8G

_Tested with January 2020 release_

```bash

wget -P ./data ftp://ftp.ncbi.nlm.nih.gov/pub/clinvar/xml/clinvar_variation/ClinVarVariationRelease_00-latest.xml.gz


## Example command lines

# Example indexing times we have seen; 40m, 60m, 3h:20m, 80m
./nosqlbiosets/variation/clinvar.py --dbtype MongoDB\
   ./data/ClinVarVariationRelease_00-latest.xml.gz

# Example indexing times we have seen; 110m, 206m, 5h:06m, 216m
./nosqlbiosets/variation/clinvar.py --dbtype Elasticsearch\
   ./data/ClinVarVariationRelease_00-latest.xml.gz --esindex clinvarvariation

```

## TODO
- All numeric types should be represented as numbers
