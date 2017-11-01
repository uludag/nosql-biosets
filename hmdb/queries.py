#!/usr/bin/env python
""" Queries with DrugBank data indexed with MongoDB """
import unittest

import networkx as nx

from nosqlbiosets.dbutils import DBconnection

DOCTYPE = 'drugbankdrug'  # MongoDB collection name


# todo: move tests to a test class
class QueryDrugBank(unittest.TestCase):
    index = "drugbank"
    db = "MongoDB"
    dbc = DBconnection(db, index)
    mdb = dbc.mdbi

    def query(self, qc, projection=None, limit=0):
        print(self.db)
        print("Querying with clause '%s'" % (str(qc)))
        c = self.mdb[DOCTYPE].find(qc, projection=projection, limit=limit)
        return c

    def distinctquery(self, key, qc=None, sort=None):
        r = self.dbc.mdbi[DOCTYPE].distinct(key, filter=qc, sort=sort)
        return r

    def test_distinct_classes(self):
        key = "classification.class"
        names = self.distinctquery(key)
        self.assertIn("Carboxylic Acids and Derivatives", names)
        assert len(names) == 242

    def test_distinct_pfam_classes(self):
        key = "transporters.polypeptide.pfams.pfam.name"
        names = self.distinctquery(key)
        self.assertIn("Alpha_kinase", names)
        assert len(names) == 86

    def test_distinct_go_classes(self):
        key = "transporters.polypeptide.go-classifiers." \
              "go-classifier.description"
        names = self.distinctquery(key)
        self.assertIn("lipid transport", names)
        assert len(names) == 1114

    def test_distinct_atc_codes(self):
        key = "atc-codes.level.#text"
        atc_codes = self.distinctquery(key)
        self.assertIn("Direct thrombin inhibitors", atc_codes)
        assert len(atc_codes) == 940
        key = "atc-codes.code"
        atc_codes = self.distinctquery(key)
        self.assertIn("A10AC04", atc_codes)
        assert len(atc_codes) == 3470

    def test_query_atc_codes(self):
        project = {"atc-codes": 1}
        qc = {"_id": "DB00001"}
        r = list(self.query(qc, projection=project))
        expected = "ANTITHROMBOTIC AGENTS"
        assert r[0]["atc-codes"][0]["level"][1]["#text"] == expected
        qc = {"_id": "DB00945"}
        r = list(self.query(qc, projection=project))
        expected = "C10BX04"
        assert r[0]["atc-codes"][0]["code"] == expected

    def test_query_drug_interactions(self):
        project = {"drug-interactions": 1}
        qc = {"_id": "DB00001"}
        r = list(self.query(qc, projection=project))
        assert len(r) == 1
        interactions = r[0]["drug-interactions"]
        assert interactions[1]["drugbank-id"] == 'DB00346'
        assert interactions[1]["name"] == 'Alfuzosin'

    def get_connections(self, qc, connections):
        project = {"name": 1, connections + ".name": 1}
        r = self.query(qc, projection=project)
        interactions = list()
        for d in r:
            name = d['name']
            if connections in d:
                for t in d[connections]:
                    interactions.append((name, t['name']))
        return interactions

    def get_connections_graph(self, qc, connections):
        interactions = self.get_connections(qc, connections)
        graph = nx.MultiDiGraph(list(interactions))
        nx.write_adjlist(graph, connections + ".adjl")
        nx.write_gexf(graph, connections + ".gexf")
        return graph

    def test_drug_interactions_graph(self):
        qc = {"affected-organisms": {
            "$in": ["Hepatitis B virus"]}}
        g = self.get_connections_graph(qc, "drug-interactions")
        assert g.number_of_edges() == 274
        assert g.number_of_nodes() == 266
        assert g.number_of_selfloops() == 0

    def test_drug_targets_graph(self):
        qc = {"affected-organisms": {
            "$in": ["Hepatitis B virus"]}}
        g = self.get_connections_graph(qc, "targets")
        assert g.number_of_edges() == 15
        assert g.number_of_nodes() == 17
        assert g.number_of_selfloops() == 0
