#!/usr/bin/env python
""" Query ClinVar data indexed with MongoDB """

from nosqlbiosets.qryutils import Query


class QueryClinVar(Query):

    # Abundance of variant interpretations per submitter
    def topinterpretationspersubmitter(self, qc):
        aggq = [
            {"$match": qc},
            {"$unwind": "$InterpretedRecord.clinicalAssertion"},
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
            {'$project': {
                "abundance": 1,
                "interpretation": "$_id.interpretation",
                "submitter": "$_id.submitter",
                "_id": 0
            }}
        ]
        r = list(self.aggregate_query(aggq))
        r = {(i['interpretation'], i['submitter']): i['abundance'] for i in r}
        return r

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
