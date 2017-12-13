
# How nosql-biosets project code is structured?

This is main source folder of the project.
Code that can be used by more than one datasets are collected here.
Index/query scripts for datasets are included in subfolders.
Few dataset folders includes related datasets as well, for example,
UniProt folder includes index script for the HIPPIE dataset.

* dbutils.py: DBconnection class
* graphutils.py: Save NetworkX graphs in selected formats
* objutils.py: Update objects for better data representation in databases
* [index_csv.py](index_csv.py): Index CSV files with Elasticsearch, MongoDB
 or PostgreSQL

Example command line:
```bash
./nosqlbiosets/index_csv.py --db PostgreSQL --infile mydatafile.tsv\
  --index geneinfo --collection amyloid --delimiter $'\t'\
  --user tests --password tests
```

[geneinfo](../geneinfo) and [hmdb](../hmdb) folders were not included
in the main source folder,
we are considering to publish new Python packages with these 2 folders/names. 
