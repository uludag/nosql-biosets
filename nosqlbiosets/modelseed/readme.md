
# ModelSEEDDatabase

* [index.py](index.py) Index ModelSEEDDatabase compounds/reactions
 data with MongoDB or Elasticsearch
 
  _Last tested in Oct 2018_

## Source

https://github.com/ModelSEED/ModelSEEDDatabase


## Example command lines for downloading and indexing

```bash
# Download ModelSEEDDatabase Biochemistry files
mkdir -p data
wget -O ./data/compounds.tsv https://github.com/ModelSEED/ModelSEEDDatabase/blob/master/Biochemistry/compounds.tsv?raw=true
wget -O ./data/reactions.tsv https://github.com/ModelSEED/ModelSEEDDatabase/blob/master/Biochemistry/reactions.tsv?raw=true

# Index compounds with MongoDB, requires ~15s
./nosqlbiosets/modelseed/index.py --db MongoDB --index biosets --compoundsfile data/compounds.tsv

# Index reactions with MongoDB, requires ~20s
./nosqlbiosets/modelseed/index.py --db MongoDB --index biosets --reactionsfile data/reactions.tsv

# Index compounds with Elasticsearch, requires ~10s
./nosqlbiosets/modelseed/index.py --db Elasticsearch --index modelseed_compound --compoundsfile data/compounds.tsv 

# Index reactions with Elasticsearch, requires ~15s
./nosqlbiosets/modelseed/index.py --db Elasticsearch --index modelseed_reaction --reactionsfile data/reactions.tsv

```

MongoDB collection names are defined in index.py:  

```python
TYPE_COMPOUND = 'modelseed_compound'
TYPE_REACTION = 'modelseed_reaction'
```
