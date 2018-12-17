#!/usr/bin/env python
""" Test queries with ModelSEEDDatabase compounds/reactions data """
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
        rstatus = {"OK": 6823, "CI:1": 27, "CI:2": 175,  "CI:4": 19,
                   "CI:-2": 137,  "CI:-4": 16,
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
                self.assertAlmostEqual(len(i['kegg_ids']), rstatus[i['_id']],
                                       delta=10), i['_id']

    def test_comparewithMetaNetX_reactions(self):
        aggpl = [
            {"$match": {"status": "OK"}},
            {"$project": {"abbreviation": 1}},
            {"$match": {"abbreviation": {"$regex": "^R[0-9]*$"}}}
        ]
        r = dbc.mdbi["modelseed_reaction"].aggregate(aggpl)
        inmodelseeddb = {i['abbreviation'] for i in r}
        self.assertAlmostEqual(6823, len(inmodelseeddb), delta=30)
        aggpl = [
            {"$match": {"balance": "true"}},
            {"$project": {"xrefs": 1}},
            {"$unwind": "$xrefs"},
            {"$match": {"xrefs.lib": "kegg"}}
        ]
        r = dbc.mdbi["metanetx_reaction"].aggregate(aggpl)
        inmetanetx = {i['xrefs']['id'] for i in r}
        assert 7785 == len(inmetanetx)
        self.assertAlmostEqual(len(inmodelseeddb - inmetanetx), 668, delta=80)
        self.assertAlmostEqual(len(inmodelseeddb.union(inmetanetx)), 8453,
                               delta=100)
        self.assertAlmostEqual(6155,
                               len(inmodelseeddb.intersection(inmetanetx)),
                               delta=100)

    def test_comparewithMetaNetX_inchikeys(self):
        r = dbc.mdbi["modelseed_compound"].distinct("inchikey")
        inmodelseeddb = {i for i in r}
        self.assertAlmostEqual(18193, len(inmodelseeddb), delta=30)
        aggpl = [
            {"$match": {"source.lib": "seed"}},
            {"$group": {"_id": "$inchikey"}}
        ]
        r = dbc.mdbi["metanetx_compound"].aggregate(aggpl)
        inmetanetx = {i['_id'] for i in r}
        self.assertAlmostEqual(3248, len(inmetanetx), delta=100)
        assert 16016 == len(inmodelseeddb - inmetanetx)
        assert 19264 == len(inmodelseeddb.union(inmetanetx))
        self.assertAlmostEqual(2180,
                               len(inmodelseeddb.intersection(inmetanetx)),
                               delta=30)

    def test_modelseeddb_parse_equation(self):
        from nosqlbiosets.modelseed.query import modelseeddb_parse_equation
        eq = "(1) cpd00003[0] + (1) cpd19024[0] <=>" \
             " (1) cpd00004[0] + (3) cpd00067[0] + (1) cpd00428[0]"
        reactants, products, direction = modelseeddb_parse_equation(eq)
        assert len(reactants) == 2
        assert len(products) == 3
        assert direction == '='

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

    def test_autocomplete_metabolitenames(self):
        names = ['Xylitol', '3-Oxopropanoate', 'Formate', 'Squalene',
                 'Glycerol', 'D-Xylose']
        for name in names:
            for qterm in [name.lower(), name.upper(), name[:4]]:
                r = qry.autocomplete_metabolitenames(qterm)
                assert any(name in i['name'] for i in r), name

    def test_metabolite_networks(self):
        qc = {
            '$text': {'$search': 'glycerol'},
            'is_transport': True
        }
        mn = qry.get_metabolite_network(qc)
        assert "Glycerol" in mn.nodes
        assert len(mn.edges) == 230
        assert len(mn.nodes) == 64

        qc = {"_id": "rxn36327"}
        mn = qry.get_metabolite_network(qc)
        assert "(S)-Propane-1,2-diol" in mn.nodes

        qc = {"status": "OK", "reversibility": "<"}
        mn = qry.get_metabolite_network(qc)
        self.assertAlmostEqual(len(mn.edges), 3743, delta=100)
        self.assertAlmostEqual(len(mn.nodes), 1558, delta=100)
        assert 'Phosphate' in mn.nodes
        r = neighbors_graph(mn, "Phosphate", beamwidth=8, maxnodes=100)
        assert 88 == r.number_of_nodes()
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
        assert '4-hydroxy-4-methyl-2-oxoglutarate pyruvate-lyase' \
               ' (pyruvate-forming)' in\
               mn.get_edge_data('Parapyruvate', 'Pyruvate')['reactions']
        paths = shortest_paths(mn, 'Parapyruvate', 'Pyruvate', 10)
        assert 10 == len(paths)
        assert 2 == len(paths[0])
        self.assertAlmostEqual(len(mn.edges), 72456, delta=100)
        assert 15672 == len(mn.nodes)
        assert 'Glycerol' in mn.nodes


if __name__ == '__main__':
    unittest.main()
