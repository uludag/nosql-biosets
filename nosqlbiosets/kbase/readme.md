
# ModelSEEDDatabase

* [index_modelseed.py](index_modelseed.py) Index ModelSEED compounds/reactions
 data with MongoDB
 
  _Indexing with Elasticsearch is broken_
 
  _Last tested on 28 Nov 2017_

## Source

https://github.com/ModelSEED/ModelSEEDDatabase



```bash
# Download ModelSEEDDatabase Biochemistry files
mkdir -p data
wget -O ./data/compounds.tsv https://github.com/ModelSEED/ModelSEEDDatabase/blob/master/Biochemistry/compounds.tsv?raw=true
wget -O ./data/reactions.tsv https://github.com/ModelSEED/ModelSEEDDatabase/blob/master/Biochemistry/reactions.tsv?raw=true

# Index compounds, requires ~2m
./nosqlbiosets/kbase/index_modelseed.py --db MongoDB --index biosets --compoundsfile data/compounds.tsv
# Index reactions, requires ~2m
./nosqlbiosets/kbase/index_modelseed.py --db MongoDB --index biosets --reactionsfile data/reactions.tsv
```

