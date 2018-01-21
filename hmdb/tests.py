#!/usr/bin/env python
""" Test queries with DrugBank data indexed with MongoDB """
import unittest

from .queries import QueryDrugBank
from .queries import DOCTYPE


class TestQueryDrugBank(unittest.TestCase):
    qry = QueryDrugBank()

    def test_distinct_classes(self):
        key = "classification.class"
        names = self.qry.distinctquery(key)
        self.assertIn("Carboxylic Acids and Derivatives", names)
        assert 292 == len(names)

    def test_distinct_pfam_classes(self):
        key = "transporters.polypeptide.pfams.pfam.name"
        names = self.qry.distinctquery(key)
        self.assertIn("Alpha_kinase", names)
        assert 90 == len(names)

    def test_distinct_go_classes(self):
        key = "transporters.polypeptide.go-classifiers." \
              "go-classifier.description"
        names = self.qry.distinctquery(key)
        self.assertIn("lipid transport", names)
        assert 1148 == len(names)

    def test_distinct_drug_targets(self):
        key = "targets.name"
        names = self.qry.distinctquery(key)
        self.assertIn("Isoleucine--tRNA ligase", names)
        assert 4022 == len(names)
        key = "targets.polypeptide.gene-name"
        names = self.qry.distinctquery(key)
        self.assertIn("RNASE4", names)
        assert 3757 == len(names)
        key = "targets.polypeptide.source"
        names = self.qry.distinctquery(key)
        self.assertIn("Swiss-Prot", names)
        assert len(names) == 2
        key = "targets.actions.action"
        names = self.qry.distinctquery(key)
        actions = "inhibitor antagonist antibody activator binder intercalation"
        assert set(actions.split(' ')).issubset(names)
        assert len(names) == 49

    def test_distinct_atc_codes(self):
        key = "atc-codes.level.#text"
        atc_codes = self.qry.distinctquery(key)
        self.assertIn("Direct thrombin inhibitors", atc_codes)
        assert 939 == len(atc_codes)
        key = "atc-codes.code"
        atc_codes = self.qry.distinctquery(key)
        self.assertIn("A10AC04", atc_codes)
        assert 3496 == len(atc_codes)

    def test_query_products(self):
        key = "products.name"
        names = self.qry.distinctquery(key)
        self.assertIn("Refludan", names)
        assert 68282 == len(names)
        project = {"_id": 1}
        # Drugs with at least one approved product
        qc = {"products.approved": True}
        r = list(self.qry.query(qc, projection=project))
        assert 2695 == len(r)
        names = self.qry.distinctquery(key, qc={"products.approved": True})
        assert 68181 == len(names)
        self.assertIn("Refludan", names)
        agpl = [
            {'$unwind': '$products'},
            {'$match': qc},
            {'$group': {
                '_id': '$products.name',
                "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {'$limit': 1}
        ]
        r = list(self.qry.aggregate_query(agpl))
        assert {'_id': 'Ibuprofen', 'count': 708} == r[0]
        agpl = [
            {'$project': {
                'products': 1,
                'numberOfProducts': {'$size': "$products"}
             }},
            {'$match': qc},
            {'$group': {
                '_id': None,
                "avg": {"$avg": '$numberOfProducts'},
                "max": {"$max": '$numberOfProducts'},
            }
            },
        ]
        r = list(self.qry.aggregate_query(agpl))
        assert 10755 == r[0]['max']
        agpl = [
            {'$project': {
                'name': 1,
                'products': 1,
                'numberOfProducts': {'$size': "$products"}
            }},
            {'$match': qc},
            {'$sort': {'numberOfProducts': -1}},
            {'$limit': 1},
            {'$project': {'name': 1, 'numberOfProducts': 1}}
        ]
        r = list(self.qry.aggregate_query(agpl))
        assert 'Octinoxate' == r[0]['name']
        qc = {"products.approved": False}
        r = list(self.qry.query(qc, projection=project))
        assert 708 == len(r)

    def test_query_atc_codes(self):
        project = {"atc-codes": 1}
        qc = {"_id": "DB00001"}
        r = list(self.qry.query(qc, projection=project))
        expected = "ANTITHROMBOTIC AGENTS"
        assert r[0]["atc-codes"][0]["level"][1]["#text"] == expected
        qc = {"_id": "DB00945"}
        r = list(self.qry.query(qc, projection=project))
        expected = "C10BX04"
        assert r[0]["atc-codes"][0]["code"] == expected

    def test_query_drug_interactions(self):
        project = {"drug-interactions": 1}
        qc = {"_id": "DB00001"}
        r = list(self.qry.query(qc, projection=project))
        assert len(r) == 1
        interactions = r[0]["drug-interactions"]
        assert interactions[1]["drugbank-id"] == 'DB00346'
        assert interactions[1]["name"] == 'Alfuzosin'

    def test_drug_interactions_graph(self):
        qc = {"affected-organisms": {
            "$in": ["Hepatitis B virus"]}}
        g = self.qry.get_connections_graph(qc, "drug-interactions")
        assert 435 == g.number_of_edges()
        assert 428 == g.number_of_nodes()
        assert g.number_of_selfloops() == 0

    def test_drug_targets_graph(self):
        from os import unlink
        qc = {'$text': {'$search': 'defensin'}}
        gfile = "defensin-targets.xml"
        g = self.qry.get_connections_graph(qc, "targets", gfile)
        assert g.number_of_edges() == 68
        assert g.number_of_nodes() == 62
        unlink(gfile)
        gfile = "defensin-enzymes.d3js.json"
        g = self.qry.get_connections_graph(qc, "enzymes", gfile)
        assert g.number_of_edges() == 1
        assert g.number_of_nodes() == 2
        unlink(gfile)
        gfile = "defensin-transporters.json"
        g = self.qry.get_connections_graph(qc, "transporters", gfile)
        assert g.number_of_edges() == 0
        assert g.number_of_nodes() == 0
        unlink(gfile)
        gfile = "defensin-carriers.gml"
        g = self.qry.get_connections_graph(qc, "carriers", gfile)
        assert g.number_of_edges() == 0
        assert g.number_of_nodes() == 0
        unlink(gfile)

    def test_drug_targets_graph_merge(self):
        import networkx as nx
        qc = {'$text': {'$search': 'lipid'}}
        g1 = self.qry.get_connections_graph(qc, "targets")
        assert 2914 == g1.number_of_edges()
        assert 2061 == g1.number_of_nodes()
        g2 = self.qry.get_connections_graph(qc, "enzymes")
        assert 594 == g2.number_of_edges()
        assert 260 == g2.number_of_nodes()
        g3 = self.qry.get_connections_graph(qc, "transporters")
        assert 464 == g3.number_of_edges()
        assert 213 == g3.number_of_nodes()
        g4 = self.qry.get_connections_graph(qc, "carriers")
        assert 101 == g4.number_of_edges()
        assert 93 == g4.number_of_nodes()
        g = nx.compose_all([g1, g2, g3, g4], "lipid network")
        assert 4050 == g.number_of_edges()
        assert 2220 == g.number_of_nodes()

    def test_get_allnetworks(self):
        tests = [
            ({}, 22927, 11376),
            ({"name": "Acetaminophen"}, 26, 27),
            ({'$text': {'$search': 'lipid'}}, 3977, 2220)
            ]
        for qc, nedges, nnodes in tests:
            g = self.qry.get_allnetworks(qc)
            assert nedges == g.number_of_edges()
            assert nnodes == g.number_of_nodes()

    def test_drug_enzymes_graph(self):
        qc = {"affected-organisms": {
            "$in": ["Hepatitis B virus"]}}
        g = self.qry.get_connections_graph(qc, "enzymes")
        assert g.number_of_edges() == 13
        assert g.number_of_nodes() == 14
        assert g.number_of_selfloops() == 0

    def test_target_genes(self):
        agpl = [
            {'$match': {'$text': {'$search': 'defensin'}}},
            {'$project': {
                'targets.polypeptide': 1}},
            {'$unwind': '$targets'},
            {'$group': {
                '_id': '$targets.polypeptide.gene-name', "count": {"$sum": 1}}}
        ]
        r = self.qry.aggregate_query(agpl)
        genes = [c['_id'] for c in r]
        assert len(genes) == 43
        self.assertIn('PRSS3', genes)

    def test_connected_drugs(self):
        tests = [
            ([(0, 6), (1, 11)],
             {'$match': {"name": "Ribavirin"}})
        ]
        for test, qc in tests:
            for maxdepth, nr in test:
                agpl = [
                    qc,
                    {"$graphLookup": {
                        "from": DOCTYPE,
                        "startWith": "$name",
                        "connectToField":
                            "drug-interactions.name",
                        "connectFromField":
                            "name",
                        "as": "neighbors",
                        "maxDepth": maxdepth,
                        "depthField": "depth",
                        "restrictSearchWithMatch": {
                            "classification.class":
                                "Carboxylic Acids and Derivatives"
                        }
                    }},
                    {"$unwind": "$neighbors"},
                    {'$project': {
                        'name': 1,
                        'neighbors.name': 1
                    }}
                ]
                r = self.qry.aggregate_query(agpl)
                r = [c for c in r]
                assert nr == len(r)


if __name__ == '__main__':
    unittest.main()
