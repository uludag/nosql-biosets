#!/usr/bin/env python
""" Test queries with IntEnz data indexed with MongoDB """

import json
import unittest

from .query import QueryIntEnz

qryintenz = QueryIntEnz()


class TestQueryIntEnz(unittest.TestCase):

    def test_getreactant_product_names(self):
        re = qryintenz.getreactantnames()
        pr = qryintenz.getproductnames()
        i = set(re).intersection(pr)
        print("#Chemicals both in reactants and products = %d" % len(i))
        assert "cytidine" in i

    def test_query_reactants(self):
        rnames = {
            "glycerol": ["D/L-glyceraldehyde reductase"],
            "D-threo-isocitrate": ["Isocitrate--homoisocitrate dehydrogenase"]
        }
        for name in rnames:
            enzymes = qryintenz.getenzymenames([name])
            for enzyme in enzymes:
                print(json.dumps(enzyme, indent=4))
            assert rnames[name][0] in [
                e["accepted_name"]["#text"] for e in enzymes]

    def test_query_products(self):
        rnames = {
            "2-oxoglutarate": ["Isocitrate--homoisocitrate dehydrogenase"]
        }
        for name in rnames:
            enzymes = qryintenz.query_products([name])
            for enzyme in enzymes:
                print(json.dumps(enzyme, indent=4))
            assert rnames[name][0] in [
                e["accepted_name"]["#text"] for e in enzymes]

    def test_query_names(self):
        rnames = ["Polynucleotide adenylyltransferase"]
        eids = qryintenz.query_names(rnames)
        print(eids)

    def test_query_reactantandproduct(self):
        ste = [
            ("isocitrate", "glyoxylate", "Isocitrate lyase"),
            ("2-oxoglutarate", "D-threo-isocitrate",
             "Isocitrate--homoisocitrate dehydrogenase")
        ]
        for source, target, enz in ste:
            r = qryintenz.query_reactantandproduct(source, target)
            assert len(r) >= 1
            assert enz in r


if __name__ == '__main__':
    unittest.main()
