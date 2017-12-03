
# ModelSEEDDatabase

* [index_modelseed.py](index_modelseed.py) Index ModelSEED compounds/reactions
 data with MongoDB
 
  TODO:: Indexing with Elasticsearch 6 requires different indices for reactions and compounds,
  use a meta field such as '_collection' to differentiate types _
 
  _Last tested on 28 Nov 2017_

## Source

https://github.com/ModelSEED/ModelSEEDDatabase


## Example command lines for downloading and indexing

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
