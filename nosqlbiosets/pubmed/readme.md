
# Acknowledgment

Index scripts described here for PMC and PubMed articles are based on
[`pubmed_parser`](https://github.com/titipata/pubmed_parser/) library,
we used the parsers they provided and learned from their documentation

# Index script for PMC (PubMed Central) articles

PMC was launched in 2000 as a free archive for full-text biomedical and life sciences
journal articles.

For download information for PMC articles see:
[https://github.com/titipata/pubmed_parser/wiki/Download-and-preprocess-Pubmed-Open-Access-dataset]

[index_pmc_articles.py](index_pmc_articles.py) reads and indexes archives
of PMC articles xml files.

```bash
python ./nosqlbiosets/pubmed/index_pmc_articles.py --infile /local/data/pmc/data/ --esindex pmctests --dbtype Elasticsearch
```

# Index script for PubMed articles

PubMed article documents do not include the full text of the article,
if the article is available through PMC, PMC article id is included in the 'pmc' field 

For download information see:
[https://github.com/titipata/pubmed_parser/wiki/Download-and-preprocess-MEDLINE-dataset]

[index_pubmed_articles.py](index_pubmed_articles.py) reads and indexes archives
of PubMed articles xml files.

```bash
python ./nosqlbiosets/pubmed/index_pubmed_articles.py\
  --infile /local/data/pubmed/baseline-2018-sample/pubmed20n0140.xml.gz\
  --esindex pubmedtests --dbtype Elasticsearch --host localhost --port 9200
python ./nosqlbiosets/pubmed/index_pubmed_articles.py\
  --infile /local/data/pubmed/baseline-2018-sample\
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
