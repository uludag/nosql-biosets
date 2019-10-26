# Index/query scripts for FDA Adverse Event Reporting System dataset

https://open.fda.gov/data/faers/

## Synopsis

Download all archived report records,
indivudual json files normally include 1200 reports

```bash
wget -nc https://api.fda.gov/download.json
grep drug-event download.json | cut -c 20- > drug-event.json.zip.files
xargs wget -nc < drug-event.json.zip.files
```

Index all .json.zip files in given folder

```bash
# Elasticsearch
nosqlbiosets/fda/faers.py --esindex faers-tests --infile ~/data/fda/\
   --dbtype Elasticsearch --host localhost --port 9200

# MongoDB (required about more than 1h)
nosqlbiosets/fda/faers.py --mdbcollection faers --infile ~/data/fda/\
 --dbtype MongoDB --host localhost  --mdbdb biosets
```

Update database with new reports

```bash
./nosqlbiosets/fda/faers.py --dbtype Elasticsearch\
    --infile ~/data/fda/drug-event-0001-of-0035.json --esindex faers

./nosqlbiosets/fda/faers.py --dbtype MongoDB\
    --infile ~/data/fda/drug-event-0001-of-0035.json\
    --mdbdb biosets --mdbcollection faers

```

## TODO/Ideas

* A project similar to [ClinVar Miner](https://clinvarminer.genetics.utah.edu/)
  with FAERS database would be helpful for researchers and practioners
  as well as for the public?
* Unique record id; 'safetyreportid' is not unique for all records
