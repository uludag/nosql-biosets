#!/usr/bin/env python
""" Queries with DrugBank data indexed with MongoDB """
import unittest

from nosqlbiosets.dbutils import DBconnection

DOCTYPE = 'drugbankdrug'


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
        key = "transporters.transporter.polypeptide.pfams.pfam.name"
        names = self.distinctquery(key)
        self.assertIn("Alpha_kinase", names)
        assert len(names) == 86

    def test_distinct_go_classes(self):
        key = "transporters.transporter.polypeptide.go-classifiers." \
              "go-classifier.description"
        names = self.distinctquery(key)
        self.assertIn("lipid transport", names)
        assert len(names) == 1114

    def test_distinct_atc_codes(self):
        key = "atc-codes.atc-code.level.#text"
        atc_codes = self.distinctquery(key)
        self.assertIn("Direct thrombin inhibitors", atc_codes)
        assert len(atc_codes) == 940
        key = "atc-codes.atc-code.code"
        atc_codes = self.distinctquery(key)
        self.assertIn("A10AC04", atc_codes)
        assert len(atc_codes) == 3470

    def test_get_atc_codes(self):
        project = {"atc-codes": 1}
        qc = {"_id": "DB00001"}
        r = list(self.query(qc, projection=project))
        expected = "ANTITHROMBOTIC AGENTS"
        assert r[0]["atc-codes"]["atc-code"]["level"][1]["#text"] == expected
        qc = {"_id": "DB00945"}
        r = list(self.query(qc, projection=project))
        expected = "C10BX04"
        assert r[0]["atc-codes"]["atc-code"][0]["code"] == expected

    def test_get_drug_interactions(self):
        project = {"drug-interactions": 1}
        qc = {"_id": "DB00001"}
        r = list(self.query(qc, projection=project))
        assert len(r) == 1
        interactions = r[0]["drug-interactions"]["drug-interaction"]
        assert interactions[1]["drugbank-id"] == 'DB00346'
        assert interactions[1]["name"] == 'Alfuzosin'


if __name__ == '__main__':
    unittest.main()
