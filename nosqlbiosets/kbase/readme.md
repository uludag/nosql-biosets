
# ModelSEEDDatabase

* [index_modelseed.py](index_modelseed.py) Index ModelSEEDDatabase compounds/reactions
 data with MongoDB
 
  _Last tested on March 2018_

## Source

https://github.com/ModelSEED/ModelSEEDDatabase


## Example command lines for downloading and indexing

```bash
# Download ModelSEEDDatabase Biochemistry files
mkdir -p data
wget -O ./data/compounds.tsv https://github.com/ModelSEED/ModelSEEDDatabase/blob/master/Biochemistry/compounds.tsv?raw=true
wget -O ./data/reactions.tsv https://github.com/ModelSEED/ModelSEEDDatabase/blob/master/Biochemistry/reactions.tsv?raw=true

# Index compounds with MongoDB, requires ~1m
./nosqlbiosets/kbase/index_modelseed.py --db MongoDB --index biosets --compoundsfile data/compounds.tsv

# Index reactions with MongoDB, requires ~2m
./nosqlbiosets/kbase/index_modelseed.py --db MongoDB --index biosets --reactionsfile data/reactions.tsv

# Index compounds with Elasticsearch, requires ~1m
./nosqlbiosets/kbase/index_modelseed.py --db Elasticsearch --index modelseeddb_compounds --compoundsfile data/compounds.tsv 
Reading from data/compounds.tsv

# Index reactions with Elasticsearch, requires ~1m
./nosqlbiosets/kbase/index_modelseed.py --db Elasticsearch --index modelseeddb_reactions --reactionsfile data/reactions.tsv 
Reading from data/reactions.tsv

```
