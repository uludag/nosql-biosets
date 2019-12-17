#!/usr/bin/env python
"""Queries with FDA Adverse Event Reporting System data indexed with MongoDB
"""

from nosqlbiosets.dbutils import DBconnection


class QueryFaers:

    def __init__(self, db="MongoDB", index="biosets", collection="faers",
                 **kwargs):
        self.collection = collection
        self.dbc = DBconnection(db, index, collection=collection, **kwargs)

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
        r = self.aggregate(aggq)
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
        r = self.aggregate(aggq)
        return r

    def query(self, qc, **kwargs):
        r = self.dbc.mdbi[self.collection].find(qc, **kwargs)
        return r

    def aggregate(self, aggpl):
        return self.dbc.mdbi[self.collection].aggregate(aggpl)

    def count(self, qc, **kwargs):
        r = self.dbc.mdbi[self.collection].count(qc, **kwargs)
        return r

    def distinct(self, key, qc, **kwargs):
        r = self.dbc.mdbi[self.collection].distinct(key, qc, **kwargs)
        return r
