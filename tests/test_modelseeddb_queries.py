#!/usr/bin/env python
""" Test queries with ModelSEEDDatabase reactions data """
import unittest

from nosqlbiosets.dbutils import DBconnection
from nosqlbiosets.graphutils import neighbors_graph
from nosqlbiosets.graphutils import shortest_paths
from nosqlbiosets.modelseed.query import QueryModelSEED

dbc = DBconnection("MongoDB", "biosets")
qry = QueryModelSEED()


class TestQueryModelSEEDDatabase(unittest.TestCase):

    # Finds ModelSEEDdb 'status' values for KEGG reactions
    # https://github.com/ModelSEED/ModelSEEDDatabase/tree/master/Biochemistry#reaction-status-values
    def test_kegg_reactions_in_modelseeddb(self):
        rstatus = {"OK": 6768, "CI:1": 27, "CI:2": 181,  "CI:4": 19,
                   "CI:-2": 141,  "CI:-4": 16,
                   "MI:O:1": 118, "MI:O:-1": 16, "MI:H:2/N:1/R:1": 54,
                   "MI:C:1/H:2": 32,
                   "MI:H:-1/O:1|CI:-1": 22,
                   "MI:C:6/H:10/O:5": 19,
                   "MI:H:-2/O:1": 22,
                   "MI:C:-1/H:-2": 22,
                   "MI:H:-2/N:-1/R:-1": 88,
                   "CPDFORMERROR": 257}
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
                assert rstatus[i['_id']] == len(i['kegg_ids'])

    def test_comparewithMetaNetX(self):
        aggpl = [
            {"$match": {"status": "OK"}},
            {"$project": {"abbreviation": 1}},
            {"$match": {"abbreviation": {"$regex": "^R[0-9]*$"}}}
        ]
        r = dbc.mdbi["modelseed_reaction"].aggregate(aggpl)
        inmodelseeddb = {i['abbreviation'] for i in r}
        self.assertAlmostEqual(6768, len(inmodelseeddb), delta=30)
        aggpl = [
            {"$match": {"balance": "true"}},
            {"$project": {"xrefs": 1}},
            {"$unwind": "$xrefs"},
            {"$match": {"xrefs.lib": "kegg"}}
        ]
        r = dbc.mdbi["metanetx_reaction"].aggregate(aggpl)
        inmetanetx = {i['xrefs']['id'] for i in r}
        assert 7785 == len(inmetanetx)
        assert 657 == len(inmodelseeddb - inmetanetx)
        assert 8442 == len(inmodelseeddb.union(inmetanetx))
        self.assertAlmostEqual(6100,
                               len(inmodelseeddb.intersection(inmetanetx)),
                               delta=30)

    def test_comparewithMetaNetX_inchikey(self):
        r = dbc.mdbi["modelseed_compound"].distinct("inchikey")
        inmodelseeddb = {i for i in r}
        self.assertAlmostEqual(18193, len(inmodelseeddb), delta=30)
        aggpl = [
            {"$match": {"source.lib": "seed"}},
            {"$group": {"_id": "$inchikey"}}
        ]
        r = dbc.mdbi["metanetx_compound"].aggregate(aggpl)
        inmetanetx = {i['_id'] for i in r}
        assert 3248 == len(inmetanetx)
        assert 16016 == len(inmodelseeddb - inmetanetx)
        assert 19264 == len(inmodelseeddb.union(inmetanetx))
        self.assertAlmostEqual(2180,
                               len(inmodelseeddb.intersection(inmetanetx)),
                               delta=30)

    def test_compoundnames(self):
        mids = ['cpd00191', 'cpd00047', 'cpd00100']
        descs = ['3-Oxopropanoate', 'Formate', 'Glycerol']
        esdbc = DBconnection("Elasticsearch", "modelseed_compound")
        for mid in mids:
            desc = descs.pop(0)
            assert desc == qry.getcompoundname(esdbc, mid)
            assert desc == qry.getcompoundname(dbc, mid)

    def test_textsearch_metabolites(self):
        mids = ['cpd00306', 'cpd00191', 'cpd00047', 'cpd00776',
                'cpd00100', 'cpd26831']
        names = ['Xylitol', '3-Oxopropanoate', 'Formate', 'Squalene',
                 'Glycerol', 'D-xylose']
        for mid in mids:
            name = names.pop(0)
            for qterm in [name.lower(), name.upper(), name]:
                r = qry.textsearch_metabolites(qterm)
                assert 1 <= len(r)
                assert mid in [i['_id'] for i in r]

    def test_metabolite_networks(self):
        qc = {
            '$text': {'$search': 'glycerol'},
            'is_transport': True
        }
        mn = qry.get_metabolite_network(qc)
        assert "Glycerol" in mn.nodes
        assert 194 == len(mn.edges)
        assert 58 == len(mn.nodes)

        qc = {"_id": "rxn36327"}
        mn = qry.get_metabolite_network(qc)
        assert "(S)-Propane-1,2-diol" in mn.nodes

        qc = {"status": "OK", "reversibility": "<"}
        mn = qry.get_metabolite_network(qc)
        assert 3571 == len(mn.edges)
        assert 1467 == len(mn.nodes)
        assert 'Phosphate' in mn.nodes
        r = neighbors_graph(mn, "Phosphate", beamwidth=8, maxnodes=100)
        assert 87 == r.number_of_nodes()
        r = neighbors_graph(mn, "Phosphate", beamwidth=6, maxnodes=20)
        assert 20 == r.number_of_nodes()
        r = neighbors_graph(mn, "Phosphate", beamwidth=4, maxnodes=20)
        assert 12 == r.number_of_nodes()

        qc = {}
        mn = qry.get_metabolite_network(qc)
        assert "(S)-Propane-1,2-diol" in mn.nodes
        assert "3-Hydroxypropanal" in mn.nodes
        assert mn.has_node('D-Xylose')
        assert mn.has_node('Xylitol')
        paths = shortest_paths(mn, 'D-Xylose', 'Xylitol', 10)
        assert 10 == len(paths)
        assert 4 == len(paths[0])

        assert mn.has_edge('Parapyruvate', 'Pyruvate')
        assert 'rxn00004' in\
               mn.get_edge_data('Parapyruvate', 'Pyruvate')['reactions']
        paths = shortest_paths(mn, 'Parapyruvate', 'Pyruvate', 10)
        assert 10 == len(paths)
        assert 2 == len(paths[0])
        assert 72427 == len(mn.edges)
        assert 15672 == len(mn.nodes)
        assert 'Glycerol' in mn.nodes


if __name__ == '__main__':
    unittest.main()
