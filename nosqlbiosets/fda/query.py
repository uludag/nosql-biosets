#!/usr/bin/env python
"""Queries with FDA Adverse Event Reporting System data indexed with MongoDB
"""

from nosqlbiosets.qryutils import Query


class QueryFaers(Query):

    def get_adversereactions(self, qc, limit=200):
        aggq = [
            {"$match": qc},
            {"$unwind": "$patient.reaction"},
            {"$group": {
                "_id": {
                    "reaction": "$patient.reaction.reactionmeddrapt",
                },
                "abundance": {"$sum": 1}
            }},
            {"$sort": {"abundance": -1}},
            {"$limit": limit}
        ]
        r = self.aggregate_query(aggq)
        return r

    def get_reaction_medicine_pairs(self, qc, limit=10):
        aggq = [
            {"$match": qc},
            {"$project": {"patient.reaction.reactionmeddrapt": 1,
                          "patient.drug.medicinalproduct": 1}},
            {"$unwind": "$patient.reaction"},
            {"$unwind": "$patient.drug"},
            {"$group": {
                "_id": {
                    "reaction": "$patient.reaction.reactionmeddrapt",
                    "medicine": "$patient.drug.medicinalproduct"
                },
                "abundance": {"$sum": 1}
            }},
            {"$sort": {"abundance": -1}},
            {"$limit": limit}
        ]
        r = self.aggregate_query(aggq)
        return r
