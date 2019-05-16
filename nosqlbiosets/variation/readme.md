# Index script for ClinVar Variation dataset

ClinVar is freely available, public archive of the relationships
between medically important variants and phenotypes​

https://www.ncbi.nlm.nih.gov/clinvar/​


* ~504K VariationArchive entries
* Compressed xml file size ~0.5G, uncompressed ~7G 

```bash

wget ftp://ftp.ncbi.nlm.nih.gov/pub/clinvar/xml/clinvar_variation/ClinVarVariationRelease_00-latest.xml.gz


# takes about 40 mins
./nosqlbiosets/variation/clinvar.py --dbtype MongoDB ClinVarVariationRelease_2019-05.xml


```
