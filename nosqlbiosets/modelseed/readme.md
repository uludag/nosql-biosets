
# ModelSEEDDatabase

* [index.py](index.py) Index ModelSEEDDatabase compounds/reactions
 data with MongoDB or Elasticsearch
 
  _ Last tested with 'dev' branch Dec 2019 _

## Data source

https://github.com/ModelSEED/ModelSEEDDatabase


## Example command lines for downloading and indexing

```bash
# Download ModelSEEDDatabase Biochemistry files from its 'dev' branch
mkdir -p data
wget -O ./data/compounds.tsv https://github.com/ModelSEED/ModelSEEDDatabase/blob/dev/Biochemistry/compounds.tsv?raw=true
wget -O ./data/reactions.tsv https://github.com/ModelSEED/ModelSEEDDatabase/blob/dev/Biochemistry/reactions.tsv?raw=true

# Index compounds with MongoDB, requires ~50s
./nosqlbiosets/modelseed/index.py --db MongoDB --index biosets --compoundsfile data/compounds.tsv

# Index reactions with MongoDB, requires ~60s
./nosqlbiosets/modelseed/index.py --db MongoDB --index biosets --reactionsfile data/reactions.tsv

# Index compounds with Elasticsearch, requires ~10s
./nosqlbiosets/modelseed/index.py --db Elasticsearch --index modelseed_compound --compoundsfile data/compounds.tsv 

# Index reactions with Elasticsearch, requires ~20s
./nosqlbiosets/modelseed/index.py --db Elasticsearch --index modelseed_reaction --reactionsfile data/reactions.tsv

# View network of metabolites that has reference to the keyword 'naringenin'
./nosqlbiosets/modelseed/query.py cyview naringenin
```

MongoDB collection names are defined in index.py:  

```python
TYPE_COMPOUND = 'modelseed_compound'
TYPE_REACTION = 'modelseed_reaction'
```
