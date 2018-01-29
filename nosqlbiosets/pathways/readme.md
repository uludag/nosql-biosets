
### WikiPathways

[index-wikipathways.py](index-wikipathways.py) reads and indexes
the archived WikiPathways gpml files,
without extracting them to temporary files

### SBML files

* [index_metabolic_networks.py](index_metabolic_networks.py): Index metabolic
  network files, current version was tested with BiGG SBML files and
  PSAMM yaml files

    For indexing PSAMM collection we need to install psamm library,
    (requires recent versions of setuptools library) 
    ```bash
    pip install psamm --user
    ```

- https://github.com/SBRG/bigg_models_data
- https://github.com/zhanglab/psamm-model-collection

JSON representation of SBML files provided by the COBRApy project[1] is used to
represent the metabolic network objects in databases (Elasticsearch and MongoDB).
One difference we have that the list of reaction metabolites and stoichiometry
values are stored in a list rather than in a dictionary of metabolites

Example metabolites list, in COBRApy json
 and in nosqlbiosets data representation: 
```
"metabolites": {
    "cpd14881": -1.0,
    "cpdLiG3P": -1.0
}
```

```
"metabolites": [
    {
        "st": -1.0,
        "id": "cpd14881"
    },
    {
        "st": 1.0,
        "id": "c_DLiG3P"
    }
]
```

[1] http://cobrapy.readthedocs.io/en/latest/io.html#JSON
