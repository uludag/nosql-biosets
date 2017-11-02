#!/usr/bin/env python
""" Test queries with DrugBank data indexed with MongoDB """
import unittest

from .queries import QueryDrugBank


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

    def test_distinct_atc_codes(self):
        key = "atc-codes.level.#text"
        atc_codes = self.qry.distinctquery(key)
        self.assertIn("Direct thrombin inhibitors", atc_codes)
        assert len(atc_codes) == 940
        key = "atc-codes.code"
        atc_codes = self.qry.distinctquery(key)
        self.assertIn("A10AC04", atc_codes)
        assert len(atc_codes) == 3470

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
        qc = {"affected-organisms": {
            "$in": ["Hepatitis B virus"]}}
        g = self.qry.get_connections_graph(qc, "targets")
        assert g.number_of_edges() == 15
        assert g.number_of_nodes() == 17
        assert g.number_of_selfloops() == 0


if __name__ == '__main__':
    unittest.main()
