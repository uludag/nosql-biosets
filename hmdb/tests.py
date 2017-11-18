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
        assert len(names) == 242

    def test_distinct_pfam_classes(self):
        key = "transporters.polypeptide.pfams.pfam.name"
        names = self.qry.distinctquery(key)
        self.assertIn("Alpha_kinase", names)
        assert len(names) == 86

    def test_distinct_go_classes(self):
        key = "transporters.polypeptide.go-classifiers." \
              "go-classifier.description"
        names = self.qry.distinctquery(key)
        self.assertIn("lipid transport", names)
        assert len(names) == 1114

    def test_distinct_drug_targets(self):
        key = "targets.name"
        names = self.qry.distinctquery(key)
        self.assertIn("Isoleucine--tRNA ligase", names)
        assert len(names) == 3985
        key = "targets.polypeptide.gene-name"
        names = self.qry.distinctquery(key)
        self.assertIn("RNASE4", names)
        assert len(names) == 3720
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
        assert len(atc_codes) == 940
        key = "atc-codes.code"
        atc_codes = self.qry.distinctquery(key)
        self.assertIn("A10AC04", atc_codes)
        assert len(atc_codes) == 3470

    def test_query_approved(self):
        project = {"_id": 1}
        qc = {"products.product.approved": "true"}
        r = list(self.qry.query(qc, projection=project))
        assert len(r) == 2677
        qc = {"products.product.approved": "false"}
        r = list(self.qry.query(qc, projection=project))
        assert len(r) == 473

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
        assert g.number_of_edges() == 274
        assert g.number_of_nodes() == 266
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
        assert g1.number_of_edges() == 2832
        assert g1.number_of_nodes() == 2006
        g2 = self.qry.get_connections_graph(qc, "enzymes")
        assert g2.number_of_edges() == 591
        assert g2.number_of_nodes() == 257
        g3 = self.qry.get_connections_graph(qc, "transporters")
        assert g3.number_of_edges() == 458
        assert g3.number_of_nodes() == 210
        g4 = self.qry.get_connections_graph(qc, "carriers")
        assert g4.number_of_edges() == 98
        assert g4.number_of_nodes() == 89
        g = nx.compose_all([g1, g2, g3, g4], "lipid network")
        assert g.number_of_edges() == 3956
        assert g.number_of_nodes() == 2163

    def test_get_allneighbors(self):
        tests = [
            ({}, 22648, 11298),
            ({"name": "Acetaminophen"}, 26, 27),
            ({'$text': {'$search': 'lipid'}}, 3883, 2163)
            ]
        for qc, nedges, nnodes in tests:
            g = self.qry.get_allneighbors(qc)
            assert g.number_of_edges() == nedges
            assert g.number_of_nodes() == nnodes

    def test_get_allnetworks(self):
        tests = [
            ({}, 22648, 11298),
            ({"name": "Acetaminophen"}, 26, 27),
            ({'$text': {'$search': 'lipid'}}, 3883, 2163)
            ]
        for qc, nedges, nnodes in tests:
            g = self.qry.get_allnetworks(qc)
            assert g.number_of_edges() == nedges
            assert g.number_of_nodes() == nnodes

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
            ([(0, 6), (1, 9)],
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
                assert len(r) == nr


if __name__ == '__main__':
    unittest.main()
