# How the project code is structured?

This is the main source folder of the nosql-biosets project.
Code that can be used by more than one datasets are collected here.
Index/query scripts specific to datasets are included in dataset subfolders,
for example `intenz` folder is for IntEnz dataset.
Few dataset folders include related datasets as well, for example,
`uniprot` folder includes index script for UniProt dataset and
for the protein interaction data files in PSI-MI-TAB format.

## List of files in the root folder

* [dbutils.py](dbutils.py): DBconnection class
* [graphutils.py](graphutils.py): Save NetworkX graphs in selected formats
* [objutils.py](objutils.py): Update objects for better data representation
  in databases
* [index_csv.py](index_csv.py): Index CSV files with Elasticsearch, MongoDB
  or PostgreSQL

Example command lines with `index_csv.py` script:
```bash
# MongoDB
./nosqlbiosets/index_csv.py --db MongoDB --infile mydatafile.tsv\
  --index geneinfo --collection mydatacollection --delimiter $'\t'

# Elasticsearch
./nosqlbiosets/index_csv.py --db Elasticsearch --infile mydatafile.tsv\
  --index geneinfo --collection mydataindex --delimiter $'\t'

# PostgreSQL
./nosqlbiosets/index_csv.py --db PostgreSQL --infile mydatafile.tsv\
  --index geneinfo --collection mydatatable --delimiter $'\t'\
  --user tests --password tests
```


[geneinfo](../geneinfo) and [hmdb](../hmdb) folders were not included here
but left in the project main folder;
we are considering to publish new Python packages with these 2 folders/names. 
