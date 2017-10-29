#!/usr/bin/env python
""" Queries with DrugBank data indexed with MongoDB """
import unittest

from nosqlbiosets.dbutils import DBconnection

DOCTYPE = 'drugbankdrug'


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

    def distinctquery(self, key, qc=None):
        r = self.dbc.mdbi[DOCTYPE].distinct(key, filter=qc)
        return r

    def test_distinct_classes(self):
        key = "classification.class"
        names = self.distinctquery(key)
        self.assertIn("Carboxylic Acids and Derivatives", names)

    def test_distinct_atc_codes(self):
        key = "atc-codes.atc-code.level.#text"
        atc_codes = self.distinctquery(key)
        self.assertIn("Direct thrombin inhibitors", atc_codes)
        assert len(atc_codes) == 940

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


if __name__ == '__main__':
    unittest.main()
