#!/usr/bin/env python
""" Query InterPro data indexed with MongoDB """

from nosqlbiosets.uniprot.qryinterpro import QueryInterPro


class TestQueryInterPro:
    qri = QueryInterPro("MongoDB", "biosets", "interpro")

    def test_id2names(self):
        c = {
            "PF00051": 'Kringle',
            "PF00008": 'EGF-like domain',
            "MF_01486": 'RecBCD enzyme subunit RecC',
            "PF02723": 'Envelope small membrane protein, coronavirus'
        }
        r = self.qri.id2names(list(c.keys()))
        for k, v in c.items():
            assert r[k] == v
