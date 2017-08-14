* [index_wikipathways.py](index_wikipathways.py): Index WikiPathways gpml files


* [index_metabolic_networks.py](index_metabolic_networks.py): Index metabolic
 network files, current/initial version is limited to PSAMM yaml files
 and SBML files

JSON representation of SBML files provided by the COBRApy project[1] is used to
represent the metabolic network objects in databases (Elasticsearch and MongoDB).
Except list of metabolites and stoichiometry values are stored in a list
rather than in a dictionary of metabolites

Example metabolites lists, before and after change: 
```json
"metabolites": {
    "cpd14881": -1.0,
    "cpdLiG3P": -1.0
}
```

```json
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
