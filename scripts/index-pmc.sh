#!/usr/bin/env bash
# Naive script to start index processes for each of the 8 PMC archive files
# ./nosqlbiosets/pubmed/index_pmc_articles.py also use multiple threads
# for parsing the individual xml files in the tar files
# and for indexing the article entries
set -eux pipefail

archives="comm_use.A-B.xml.tar.gz  comm_use.I-N.xml.tar.gz\
  non_comm_use.I-N.xml.tar.gz\
  non_comm_use.A-B.xml.tar.gz\
  comm_use.C-H.xml.tar.gz  comm_use.O-Z.xml.tar.gz\
  non_comm_use.C-H.xml.tar.gz  non_comm_use.O-Z.xml.tar.gz"


host=borgdb.cbrc.kaust.edu.sa
pmcfolder=./data/pmc

for archive in ${archives}
do
     echo "${archive}"
     nohup time python3 ./nosqlbiosets/pubmed/index_pmc_articles.py\
       --infile "${pmcfolder}/${archive}" --dbtype Elasticsearch \
       --esindex pmc --host ${host} --port 9200 >& "${archive}.log"&
     sleep 14
done
