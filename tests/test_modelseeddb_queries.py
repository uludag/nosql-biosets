#!/usr/bin/env python
""" Test queries with ModelSEEDDatabase reactions """
import unittest

from nosqlbiosets.dbutils import DBconnection

dbc = DBconnection("MongoDB", "biosets")


class TestQueryModelSEEDDatabase(unittest.TestCase):

    # Find different ModelSEEDdb 'status' values for KEGG reactions
    # https://github.com/ModelSEED/ModelSEEDDatabase/tree/master/Biochemistry#reaction-status-values
    def test_kegg_reactions_in_modelseeddb(self):
        rstatus = {"OK": 6766, "CI:1": 23, "CI:2": 178,  "CI:4": 19,
                   "CI:-2": 141,  "CI:-4": 16,
                   "MI:O:1": 118, "MI:O:-1": 16, "MI:H:2/N:1/R:1": 54,
                   "MI:C:1/H:2": 32,
                   "MI:H:-1/O:1|CI:-1": 22,
                   "MI:C:6/H:10/O:5": 19,
                   "MI:H:-2/O:1": 22,
                   "MI:C:-1/H:-2": 22,
                   "MI:H:-2/N:-1/R:-1": 88,
                   "CPDFORMERROR": 269}
        aggpl = [
            {"$project": {"abbreviation": 1, "status": 1}},
            {"$match": {"abbreviation": {"$regex": "^R[0-9]*$"}}},
            {"$group": {
                "_id": "$status",
                "kegg_ids": {"$addToSet": "$abbreviation"}
            }}

        ]
        r = dbc.mdbi["modelseed_reaction"].aggregate(aggpl)
        for i in r:
            # 769 different status values, check only frequent values
            if len(i['kegg_ids']) > 15:
                assert len(i['kegg_ids']) == rstatus[i['_id']]

    def test_comparewithMetaNetX(self):
        aggpl = [
            {"$match": {"status": "OK"}},
            {"$project": {"abbreviation": 1}},
            {"$match": {"abbreviation": {"$regex": "^R[0-9]*$"}}}
        ]
        r = dbc.mdbi["modelseed_reaction"].aggregate(aggpl)
        inmodelseeddb = {i['abbreviation'] for i in r}
        assert len(inmodelseeddb) == 6766
        aggpl = [
            {"$match": {"balance": "true"}},
            {"$project": {"xrefs": 1}},
            {"$unwind": "$xrefs"},
            {"$match": {"xrefs.lib": "kegg"}}
        ]
        r = dbc.mdbi["metanetx_reaction"].aggregate(aggpl)
        inmetanetx = {i['xrefs']['id'] for i in r}
        assert len(inmetanetx) == 7950
        assert len(inmodelseeddb - inmetanetx) == 501
        assert len(inmodelseeddb.union(inmetanetx)) == 8451
        assert len(inmodelseeddb.intersection(inmetanetx)) == 6265


if __name__ == '__main__':
    unittest.main()
