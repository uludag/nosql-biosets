
# Acknowledgment

Index scripts described here for PMC and PubMed articles are based on
[`pubmed_parser`](https://github.com/titipata/pubmed_parser/) library,
we used the parsers they provided and learned from their documentation

For installing pubmed_parser see
[https://github.com/titipata/pubmed_parser/#install-package](
https://github.com/titipata/pubmed_parser/#install-package
)

# Index script for PMC (PubMed Central) articles

PMC was launched in 2000 as a free archive for full-text biomedical and life sciences
journal articles.
Since then, "PMC has grown from comprising only two journals,
 Proceedings of the National Academy of Sciences
 and Molecular Biology of the Cell,
 to an archive of articles from thousands of journals"
[[https://www.ncbi.nlm.nih.gov/pmc/about/intro/](https://www.ncbi.nlm.nih.gov/pmc/about/intro/)].

For download we used [PMC FTP service](https://www.ncbi.nlm.nih.gov/pmc/tools/ftp/)
[Open Access bulk files folder](ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk)

[index_pmc_articles.py](index_pmc_articles.py) reads and indexes archives
of PMC articles xml files with multiple threads.

Example snippets to download and index xml archive files
```bash
# 8 archive files with total size of ~42G, they look updated on a daily basis
wget -nc -P ./data/pmc ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/*.xml.tar.gz

# Index all archive files in ./data/pmc folder with Elasticsearch with 'pmc' index name
python ./nosqlbiosets/pubmed/index_pmc_articles.py --infile ./data/pmc/\
 --esindex pmc --dbtype Elasticsearch

# Index with MongoDB 'biosets' database with 'pmc' collection name
python ./nosqlbiosets/pubmed/index_pmc_articles.py --infile /local/data/pmc/data/\
 --mdbdb biosets --mdbcollection pmc --dbtype Elasticsearch
```
Note: our concentration for PubMed and PMC is with Elasticsearch indexing,
indexing with MongoDB have not been tested with the latest changes yet

Current Open Acces bulk files folder includes 8 XML archive files,
 [../../scripts/index-pmc.sh](../../scripts/index-pmc.sh) script can be used to start
 parallel index processe for each archive file,
 typically takes about 10 hours to complete all 8 index jobs.  

# Index script for PubMed articles

PubMed article records do not include the full text of the articles, they include
bibliographic informatin.
If an article is available through PMC, PMC article id is included in the 'pmc' field


[index_pubmed_articles.py](index_pubmed_articles.py) reads and indexes archives
of PubMed articles xml files.

Example snippets to download and index xml archive files
```bash

wget -nc -P ./data/pubmed ftp://ftp.ncbi.nlm.nih.gov/pubmed/baseline/*.xml.gz
# 1016 xml archive files in 2020 baseline folder, total file size 28G
wget -nc -P ./data/pubmed/updatefiles ftp://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/*.xml.gz
# ~80 xml archive files, 26 Feb 2020,  total file size ~2.1G

# Index all XML archive files in ./data/pubmed folder
# 30419056 article records in 2020 baseline folder
python ./nosqlbiosets/pubmed/index_pubmed_articles.py\
  --infile ./data/pubmed\
  --esindex pubmed --dbtype Elasticsearch --host localhost --port 9200

# Index all XML archive files in ./data/pubmed/updatefiles folder
# ~220K article records in updates folder (26 Feb 2020)
# indexing takes about half an hour
python ./nosqlbiosets/pubmed/index_pubmed_articles.py\
  --infile ./data/pubmed/updatefiles\
  --esindex pubmed --dbtype Elasticsearch --host localhost --port 9200

# Index individual XML archive files in ./data/pubmed folder
python ./nosqlbiosets/pubmed/index_pubmed_articles.py\
  --infile ./data/pubmed/pubmed20n0060.xml.gz\
  --esindex pubmedtests --dbtype Elasticsearch --host localhost --port 9200
```

Following information for PubMed is a partial copy of the article at
[https://www.nlm.nih.gov/bsd/difference.html]:
 
PubMed citations come from

 1) MEDLINE indexed journals
 2) journals/manuscripts deposited in PMC
 3) NCBI Bookshelf
Both MEDLINE and other PubMed citations may have links to full-text articles
or manuscripts in PMC, NCBI Bookshelf, and publishers' Web sites.

## MEDLINE

MEDLINE® is the National Library of Medicine® (NLM®) journal citation database.
Started in the 1960s, it now provides more than 26 million references
to biomedical and life sciences journal articles back to 1946.
MEDLINE includes citations from more than 5,200 scholarly journals
published around the world.

MEDLINE (Medical Literature Analysis and Retrieval System Online, or MEDLARS Online)
is a bibliographic database of life sciences and biomedical information.
It includes bibliographic information for articles from academic journals
covering medicine, nursing, pharmacy, dentistry, veterinary medicine, and health care.
MEDLINE also covers much of the literature in biology and biochemistry

## PubMed

The MEDLINE database is directly searchable as a subset of the PubMed® database.
In addition to the comprehensive journal selection process,
what sets MEDLINE apart from the rest of PubMed
is the added value of using the NLM controlled vocabulary,
Medical Subject Headings (MeSH®), to index citations

PubMed has been available since 1996.
Its more than 30 million references include the MEDLINE database
