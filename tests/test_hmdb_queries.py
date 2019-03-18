#!/usr/bin/env python
""" Test queries with HMDB metabolites and proteins, indexed with MongoDB """
import json
import os
import unittest

import networkx as nx

from hmdb.index import DOCTYPE_METABOLITE, DOCTYPE_PROTEIN
from hmdb.queries import QueryHMDB
from nosqlbiosets.dbutils import DBconnection

EXAMPLES = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                        '../docs/example-graphs/')


class TestQueryHMDB(unittest.TestCase):
    index = "biosets"
    db = "MongoDB"
    dbc = DBconnection(db, index)
    mdb = dbc.mdbi
    qry = QueryHMDB(index=index)

    def query(self, qc, doctype=None, size=20):
        print(self.db)
        print("Querying '%s' records with clause '%s'" % (doctype, str(qc)))
        c = self.mdb[doctype].find(qc, limit=size)
        r = [doc for doc in c]
        c.close()
        return r

    def test_ex_keggids_query(self):
        keggids = ['C19962']
        if self.dbc.db == 'MongoDB':
            qc = {"kegg_id": ' '.join(keggids)}
            hits = self.query(qc, DOCTYPE_METABOLITE)
            hmdbids = [c['_id'] for c in hits]
            assert 'HMDB0000305' in hmdbids

    def test_ex_text_search(self):
        qterms = ['ATP']
        qc = {'$text': {'$search': ' '.join(qterms)}}
        hits = self.query(qc, DOCTYPE_METABOLITE)
        mids = [c['_id'] for c in hits]
        self.assertEqual(len(mids), 20)

    def test_ex_query_groupby(self):
        agpl = [
            {'$match': {'$text': {'$search': 'bacteriocin'}}},
            {'$group': {
                '_id': '$taxonomy.super_class', "count": {"$sum": 1}}}
        ]
        cr = self.mdb[DOCTYPE_METABOLITE].aggregate(agpl)
        r = [c['_id'] for c in cr]
        self.assertIn('Organoheterocyclic compounds', r)

    def test_ex_query__related_entries_stat(self):
        # (2, 846), (3, 591), (4, 563), (5, 279), (6, 202), (7, 149), (8, 121),
        # (9, 109), (11, 81), (10, 77), (12, 49), (13, 45), (32, 31), (14, 29),
        # (17, 23), (15, 21), (23, 21), (16, 20), (1278, 18), (41, 18),
        # (19, 17), (518, 15), (1281, 15), (18, 15), (843, 14), (42, 14),
        # (897, 14), (43, 13), (20, 13), (25, 13), (38, 13), (22, 12), ...
        # (1279, 12), (24, 11), (2618, 10), (44, 9), (124, 9), (36, 8), (40, 8)
        agpl = [
            {'$match': {
                'metabolite_associations.metabolite.0': {"$exists": True}
                # '$type': 'array'
            }},
            {'$group': {
                '_id': {'$size': '$metabolite_associations.metabolite'},
                "count": {"$sum": 1}
            }},
            {"$sort": {"count": -1}},
        ]
        hits = self.mdb[DOCTYPE_PROTEIN].aggregate(agpl)
        r = [(c['_id'], c['count']) for c in hits]
        print(r)
        assert (2, 846) == r[0]  # total number of proteins is 5702
        assert (3, 591) == r[1]
        assert (4, 563) == r[2]
        # (34, 13636), (2, 1453), (43, 971), (78, 955), (130, 803), (3, 759),
        # (115, 440), (41, 408), (80, 363), (4, 357), (30, 233), (5, 209),
        # (8, 186), (26, 179), (6, 171), (9, 144), (7, 136), (72, 126),
        # (44, 75), (10, 74), (25, 55), (18, 53), (19, 52), (11, 51), (131, 40),
        # (12, 39), (14, 35), (46, 32), (50, 29), (13, 27), (66, 24), ...
        # (1040, 1), (261, 1), (686, 1), (129, 1), (179, 1), (788, 1), (87, 1)
        agpl = [
            {'$match': {
                'protein_associations.protein': {
                    '$type': 'array'}}},
            {'$group': {
                '_id': {'$size': '$protein_associations.protein'},
                "count": {"$sum": 1}
            }},
            {"$sort": {"count": -1}},
        ]
        hits = self.mdb[DOCTYPE_METABOLITE].aggregate(agpl)
        r = [(c['_id'], c['count']) for c in hits]
        print(r)
        assert (34, 13636) == r[0]  # total number of metabolites is 114400
        assert (2, 1457) == r[1]
        assert (43, 971) == r[2]

    def test_ex_query_lookup(self):
        agpl = [
            {'$match': {'$text': {'$search': 'antibiotic'}}},
            {'$match': {
                "taxonomy.super_class": "Phenylpropanoids and polyketides"}},
            {'$lookup': {
                'from': DOCTYPE_PROTEIN,
                'localField': 'accession',
                'foreignField': 'metabolite_associations.metabolite.accession',
                'as': 'protein_docs'
            }},
            {"$match": {
                "protein_docs.4": {"$exists": True}}}
        ]
        r = list(self.mdb[DOCTYPE_METABOLITE].aggregate(agpl))
        assert 2 == len(r)
        genes = [{pr['gene_name'] for pr in metabolite['protein_docs']}
                 for metabolite in r]
        assert {'CYP3A4'} == genes[0].intersection(genes[1])

    def test_connected_metabolites__example_graph(self):
        qc = {'$text': {'$search': 'albumin'}}
        connections = self.qry.getconnectedmetabolites(qc, max_associations=10)
        r = self.qry.get_connections_graph(connections, json.dumps(qc))
        print(nx.info(r))
        from nosqlbiosets.graphutils import save_graph
        save_graph(r, EXAMPLES + 'hmdb-ex-graph.json')
        assert 49 == len(r)

    def test_connected_metabolites(self):
        tests = [
            # query, expected results with/out maximum associations limit
            ({'$text': {'$search': 'methicillin'}},
             (125, 1, 2, 72), (0, 0, 0, 0)),
            ({'$text': {'$search': 'bilirubin'}},
             (16728, 7, 37, 2689), (188, 3, 15, 66)),
            ({'$text': {'$search': 'albumin'}},
             (2498, 6, 24, 822), (68, 4, 12, 41)),
            ({'$text': {'$search': 'cofactor'}},
             (33937, 63, 543, 8819), (5272, 57, 461, 863)),
            ({"taxonomy.class": "Quinolines and derivatives"},
             (25242, 33, 65, 5605), (954, 24, 30, 282)),
            ({"taxonomy.sub_class": "Pyrroloquinolines"},
             (0, 0, 0, 0), (0, 0, 0, 0)),
            ({'taxonomy.substituents': "Pyrroloquinoline"},
             (8662, 10, 23, 720), (896, 7, 10, 75)),
            ({'accession': 'HMDB0000678'},
             (366, 1, 4, 163), (0, 0, 0, 0))
        ]
        for qc, a, b in tests:
            for c, max_associations in [[a, -1], [b, 30]]:
                # max_associations: -1, 30
                npairs, u_, g_, v_ = c
                r = list(self.qry.getconnectedmetabolites(
                    qc, max_associations=max_associations))
                u = {i['m1'] for i in r}
                g = {i['gene'] for i in r}
                v = {i['m2'] for i in r}
                self.assertAlmostEqual(npairs, len(r), delta=300, msg=qc)
                self.assertAlmostEqual(len(u), u_, delta=30, msg=qc)
                self.assertAlmostEqual(len(g), g_, delta=30, msg=qc)
                self.assertAlmostEqual(len(v), v_, delta=30, msg=qc)

    def test_metabolites_protein_functions(self):
        # Functions of associated proteins for selected set of Metabolites
        tests = [
            ({"$text": {"$search": 'saffron'}},
             "Involved in sulfotransferase activity"),
            ({"protein_associations.protein.gene_name": {
                "$in": ['ABAT', 'CPT1C']}},
             "Involved in acyltransferase activity")
        ]
        for qc, gfunc in tests:
            r = self.qry.metabolites_protein_functions(qc)
            assert gfunc in (i['_id'] for i in r)


if __name__ == '__main__':
    unittest.main()
