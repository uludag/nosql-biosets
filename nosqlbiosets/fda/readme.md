# Index/query scripts for FDA Adverse Event Reporting System dataset

Note: this work is at its early stages

https://open.fda.gov/data/faers/

## Synopsis

Download all archived report records,
indivudual json files normally include 1200 reports

```bash
wget -nc https://api.fda.gov/download.json
grep drug-event download.json | cut -c 20- > drug-event.json.zip.files

# File names are not unique, some archive file names exist in multiple years
# For this reason we download with folder option '-x'
xargs wget -x -nc --no-host-directories --cut-dirs=0 < drug-event.json.zip.files
```

Index all .json.zip files in given folder

```bash
# Elasticsearch
./nosqlbiosets/fda/faers.py --esindex faers --infile\
   ~/data/fda/faers/drug/event/2019q1\
   --dbtype Elasticsearch --recreateindex true\
   --host localhost

# MongoDB
nosqlbiosets/fda/faers.py --mdbcollection faers --infile ~/data/fda/\
 --dbtype MongoDB --host localhost  --mdbdb biosets
./nosqlbiosets/fda/faers.py --mdbcollection faers\
  --infile ~/data/fda/faers/drug/event/2019q1 --recreateindex true\
  --dbtype MongoDB --host localhost --mdbdb biosets
```

Update database with new reports files

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
