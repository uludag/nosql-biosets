

Index/query scripts for FDA Adverse Event Reporting System dataset

https://open.fda.gov/data/faers/

## Synopsis

Current index script works only for indivudual json files
that mormally include 1200 reports
```
./nosqlbiosets/fda/faers.py --dbtype Elasticsearch\
    --infile ~/data/fda/drug-event-0001-of-0035.json --esindex faers

./nosqlbiosets/fda/faers.py --dbtype Elasticsearch\
    --infile ~/data/fda/drug-event-0001-of-0035.json --mdbdb biosets --mdbcollection faers
```

## Data download

We do not yet have a download/index script for complete dataset
Following what we have at this moment:

wget https://api.fda.gov/download.json
grep drug-event download.json | grep 2019 |  cut -c 20- > 2019.txt
xargs wget < 2019.txt
ls  *.json.zip |  xargs -n 1 jar -xvf


## TODO/Ideas
* A project similar to [ClinVar Miner](https://clinvarminer.genetics.utah.edu/)
  with FAERS database would be helpful for researchers and practioners
  as well as for the public
