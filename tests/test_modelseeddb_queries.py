#!/usr/bin/env python
""" Test queries with ModelSEEDDatabase compounds/reactions data """
import unittest

from nosqlbiosets.dbutils import DBconnection
from nosqlbiosets.graphutils import neighbors_graph, shortest_paths,\
    set_degree_as_weight
from nosqlbiosets.modelseed.query import QueryModelSEED

qry = QueryModelSEED(db="MongoDB", index="biosets")


class TestQueryModelSEEDDatabase(unittest.TestCase):

    # Finds ModelSEEDdb 'status' values for KEGG reactions
    # https://github.com/ModelSEED/ModelSEEDDatabase/tree/master/Biochemistry#reaction-status-values
    def test_kegg_reactions_in_modelseeddb(self):
        rstatus = {"OK": 6869, "CI:1": 27, "CI:2": 175,  "CI:4": 19,
                   "CI:-2": 137,  "CI:-4": 16,
                   "MI:O:1": 118, "MI:O:-1": 16, "MI:H:2/N:1/R:1": 54,
                   "MI:C:1/H:2": 32,
                   "MI:H:-1/O:1|CI:-1": 22,
                   "MI:C:6/H:10/O:5": 19,
                   "MI:H:-2/O:1": 22,
                   "MI:C:-1/H:-2": 22,
                   "MI:H:-2/N:-1/R:-1": 88,
                   "CPDFORMERROR": 224}
        aggpl = [
            {"$project": {"abbreviation": 1, "status": 1}},
            {"$match": {"abbreviation": {"$regex": "^R[0-9]*$"}}},
            {"$group": {
                "_id": "$status",
                "kegg_ids": {"$addToSet": "$abbreviation"}
            }}
        ]
        r = qry.dbc.mdbi["modelseed_reaction"].aggregate(aggpl)
        for i in r:
            # 769 different status values, check only frequent values
            if len(i['kegg_ids']) > 15:
                self.assertAlmostEqual(len(i['kegg_ids']), rstatus[i['_id']],
                                       delta=10), i['_id']

    def test_compounds_in_transport_reactions(self):
        # Test with the compounds of transport reactions
        aggpl = [
            {"$match": {
                'is_transport': True,
                "is_obsolete": False
            }},
            {"$project": {"compound_ids": 1, "status": 1}}
        ]
        r = qry.dbc.mdbi["modelseed_reaction"].aggregate(aggpl)
        r = list(r)
        assert len(r) == 3728

        cids = set()
        for i in r:
            for j in i['compound_ids'].split(';'):
                cids.add(j)
        print(len(cids))
        assert len(cids) == 2384
        qc = [
            {"$match": {
                '_id': {"$in": list(cids)}
            }},
            {"$project": {"aliases": 1, "_id":0}}
        ]
        r = qry.dbc.mdbi["modelseed_compound"].aggregate(qc)
        r = list(r)
        assert len(r) == 2384

        def aliases2keggids(a):
            if "KEGG" not in a:
                return []
            keggids = [i for i in a.split('|') if i.startswith("KEGG")][0]
            return [i for i in keggids[6:].split('; ') if i[0] == 'C']
        cids.clear()
        for c in r:
            if 'aliases' in c:
                ids = aliases2keggids(c['aliases'])
                cids = cids.union(ids)
        assert len(cids) == 1390

    def test_comparewithMetaNetX_reactions(self):
        aggpl = [
            {"$match": {"status": "OK"}},
            {"$project": {"abbreviation": 1}},
            {"$match": {"abbreviation": {"$regex": "^R[0-9]*$"}}}
        ]
        r = qry.dbc.mdbi["modelseed_reaction"].aggregate(aggpl)
        inmodelseeddb = {i['abbreviation'] for i in r}
        self.assertAlmostEqual(6859, len(inmodelseeddb), delta=300)
        aggpl = [
            {"$match": {"balance": "true"}},
            {"$project": {"xrefs": 1}},
            {"$unwind": "$xrefs"},
            {"$match": {"xrefs.lib": "kegg"}}
        ]
        r = qry.dbc.mdbi["metanetx_reaction"].aggregate(aggpl)
        inmetanetx = {i['xrefs']['id'] for i in r}
        assert 7927 == len(inmetanetx)
        self.assertAlmostEqual(len(inmodelseeddb - inmetanetx), 542, delta=80)
        self.assertAlmostEqual(len(inmodelseeddb.union(inmetanetx)), 8453,
                               delta=100)
        self.assertAlmostEqual(6317,
                               len(inmodelseeddb.intersection(inmetanetx)),
                               delta=100)

    def test_comparewithMetaNetX_inchikeys(self):
        r = qry.dbc.mdbi["modelseed_compound"].distinct("inchikey")
        inmodelseeddb = {i for i in r}
        self.assertAlmostEqual(24082, len(inmodelseeddb), delta=300)
        aggpl = [
            {"$match": {"source.lib": "seed"}},
            {"$group": {"_id": "$inchikey"}}
        ]
        r = qry.dbc.mdbi["metanetx_compound"].aggregate(aggpl)
        inmetanetx = {i['_id'] for i in r}
        self.assertAlmostEqual(3097, len(inmetanetx), delta=100)
        assert len(inmodelseeddb - inmetanetx) == 21971
        assert len(inmodelseeddb.union(inmetanetx)) == 25068
        self.assertAlmostEqual(len(inmodelseeddb.intersection(inmetanetx)),
                               2100, delta=30)

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
            assert desc == qry.getcompoundname(qry.dbc, mid)

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

    def test_metabolite_networks_neighbors(self):
        qc = {
            '$text': {'$search': 'glycerol'}
        }
        mn = qry.get_metabolite_network(qc, limit=1440)
        assert "Glycerol" in mn.nodes
        assert len(mn.edges) == 3219
        assert len(mn.nodes) == 906

        qc = {
            '$text': {'$search': 'glycerol'},
            'is_transport': True
        }
        mn = qry.get_metabolite_network(qc)
        assert "Glycerol" in mn.nodes
        assert len(mn.edges) == 228
        assert len(mn.nodes) == 64

        qc = {"_id": "rxn36327"}
        mn = qry.get_metabolite_network(qc)
        assert "(S)-Propane-1,2-diol" in mn.nodes

        qc = {"status": "OK", "reversibility": "<"}
        mn = qry.get_metabolite_network(qc)
        self.assertAlmostEqual(len(mn.edges), 2027, delta=100)
        self.assertAlmostEqual(len(mn.nodes), 961, delta=100)
        assert 'Phosphate' in mn.nodes
        r = neighbors_graph(mn, "Phosphate", beamwidth=8, maxnodes=100)
        assert r.number_of_nodes() == 95
        r = neighbors_graph(mn, "Phosphate", beamwidth=6, maxnodes=20)
        assert r.number_of_nodes() == 20
        r = neighbors_graph(mn, "Phosphate", beamwidth=4, maxnodes=20)
        assert r.number_of_nodes() == 20

    def test_metabolite_networks_shortespaths(self):
        qc = {}
        mn = qry.get_metabolite_network(qc)
        assert "(S)-Propane-1,2-diol" in mn.nodes
        assert "3-Hydroxypropanal" in mn.nodes
        assert mn.has_node('D-Xylose')
        assert mn.has_node('Xylitol')
        assert mn.has_edge('Parapyruvate', 'Pyruvate')
        assert '4-hydroxy-4-methyl-2-oxoglutarate pyruvate-lyase' \
               ' (pyruvate-forming)' in\
               mn.get_edge_data('Parapyruvate', 'Pyruvate')['reactions']
        self.assertAlmostEqual(len(mn.edges), 97416, delta=1000)
        self.assertAlmostEqual(len(mn.nodes), 20510, delta=1000)
        assert 'Glycerol' in mn.nodes

        paths = shortest_paths(mn, 'D-Xylose', 'Xylitol', 40)
        assert len(paths) == 40
        assert len(paths[0]) == 3
        paths = shortest_paths(mn, 'Parapyruvate', 'Pyruvate', 40)
        assert len(paths) == 40
        assert len(paths[0]) == 2

        set_degree_as_weight(mn)
        paths = shortest_paths(mn, 'D-Xylose', 'Xylitol', 10,
                               cutoff=8, weight='weight')
        assert len(paths) == 6
        assert 8 == len(paths[0])
        paths = shortest_paths(mn, 'Parapyruvate', 'Pyruvate', 20,
                               weight='weight')
        assert 9 == len(paths)
        assert 2 == len(paths[0])


if __name__ == '__main__':
    unittest.main()
