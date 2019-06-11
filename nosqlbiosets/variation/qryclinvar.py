#!/usr/bin/env python
""" Query ClinVar data indexed with MongoDB """

from nosqlbiosets.dbutils import DBconnection


class QueryClinVar:

    def __init__(self, db, index, doctype, **kwargs):
        self.index = index
        self.doctype = doctype
        self.dbc = DBconnection(db, self.index, **kwargs)

    def query(self, qc, projection=None, limit=0):
        c = self.dbc.mdbi[self.doctype].find(qc, projection=projection,
                                             limit=limit)
        return c

    def count(self, qc, **kwargs):
        n = self.dbc.mdbi[self.doctype].count(qc, **kwargs)
        return n

    def distinct(self, key, qc, **kwargs):
        r = self.dbc.mdbi[self.doctype].distinct(key, qc, **kwargs)
        return r

    def aggregate_query(self, agpl, **kwargs):
        r = self.dbc.mdbi[self.doctype].aggregate(agpl, **kwargs)
        return r

    # Abundance of variant interpretations per submitter
    def topinterpretationspersubmitter(self, qc, limit=10):
        aggq = [
            {"$match": qc},
            {"$group": {
                "_id": {
                    "interpretation": "$InterpretedRecord.Interpretations."
                                      "Interpretation.Description",
                    "submitter": "$InterpretedRecord.clinicalAssertion."
                                 "ClinVarAccession.SubmitterName",
                },
                "abundance": {"$sum": 1}
            }},
            {"$sort": {"abundance": -1}},
            {"$limit": limit}
        ]
        cr = self.aggregate_query(aggq)
        return cr

    # Abundance of variant interpretations per gene
    def topinterpretationspergene(self, qc, limit=10):
        aggq = [
            {"$match": qc},
            {"$group": {
                "_id": {
                    "gene": '$InterpretedRecord.SimpleAllele.GeneList.Gene.'
                            'Symbol',
                    "desc": "$InterpretedRecord.Interpretations."
                            "Interpretation.Description"
                },
                "abundance": {"$sum": 1}
            }},
            {"$sort": {"abundance": -1}},
            {"$limit": limit}
        ]
        cr = self.aggregate_query(aggq)
        return cr
