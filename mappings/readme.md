
Elasticsearch mappings define  how a document, and the fields it contains,
are stored and indexed [1]

We use default Elasticsearch mappings for most of the datasets in the project.
Following custom mapping files are for PubTator and PubChem datasets:

* [PubTator gene2pub mappings](pubtator.json)
* [PubChem Bioassays mappings](pubchem-bioassays.json)

Above mapping files are for Elasticsearch-5,
Custom mapping files for Elasticsearch-2 use `-es2.json` file name suffix.


[1] https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping.html

