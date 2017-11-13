#!/usr/bin/env python
""" Test queries with HIPPIE interaction data indexed with MongoDB """
import unittest

from nosqlbiosets.dbutils import DBconnection

DOCTYPE = "interaction"  # MongoDB collection name


class TestHIPPIE(unittest.TestCase):
    index = "mitab"
    db = "MongoDB"
    dbc = DBconnection(db, index)
    mdb = dbc.mdbi

    def query(self, qc, projection=None, limit=0):
        c = self.mdb[DOCTYPE].find(qc, projection=projection, limit=limit)
        return c

    def aggregate_query(self, agpl, allowdiskuse=False):
        r = self.mdb[DOCTYPE].aggregate(agpl, allowDiskUse=allowdiskuse)
        return r

    def distinctquery(self, key, qc=None, sort=None):
        r = self.dbc.mdbi[DOCTYPE].distinct(key, filter=qc, sort=sort)
        return r

    def test_distinct_ids(self):
        key = "idsA"
        names = self.distinctquery(key)
        for name in names:
            assert name == '-' or name.startswith("uniprotkb:")
        assert len(names) == 14362
        key = "idA"
        names = self.distinctquery(key)
        for name in names:
            assert name == '-' or name.startswith("entrez gene:")
        assert len(names) == 14334
        key = "idsB"
        names = self.distinctquery(key)
        for name in names:
            assert name == '-' or name.startswith("uniprotkb:")
        assert len(names) == 15748
        key = "idB"
        names = self.distinctquery(key)
        for name in names:
            assert name == '-' or name.startswith("entrez gene:")
        assert len(names) == 15996

    def test_neighbors_neighbors(self):
        key = "idB"
        tests = [
            (2, 432, 25626, {"source": "biogrid", "conf": {"$gt": 0.93}}),
            (2, 4, 4, {"idA": "entrez gene:374918"})
        ]
        for a, b, c, qc in tests:
            idbs = self.distinctquery(key, qc=qc)
            assert len(idbs) == a
            qc = {"idA": {"$in": idbs}}
            r = self.query(qc)
            idbs = [c['idB'] for c in r]
            assert len(idbs) == b
            qc = {"idA": {"$in": idbs}}
            r = self.query(qc)
            idbs = [c['idB'] for c in r]
            assert len(idbs) == c

    def get_connections(self, qc):
        project = {"idsA": 1, "idsB": 1}
        r = self.query(qc, projection=project)
        interactions = list()
        for d in r:
            id1 = d['idsA']
            id2 = d['idsB']
            interactions.append((id1, id2))
        return interactions

    def test_graph_construction(self):
        import networkx as nx
        tests = [
            (448, {"source": "biogrid", "conf": {"$gt": 0.85}}),
        ]
        for ner, qc in tests:
            r = self.get_connections(qc=qc)
            idbs = [b for a, b in r]
            assert len(idbs) == ner
            g = nx.MultiDiGraph(r)
            assert g.number_of_edges() == 448
            assert g.number_of_nodes() == 577
            for n in g:
                nx.single_source_shortest_path_length(g, n, cutoff=4)
                break

    def test_connected_proteins(self):
        tests = [
            (1, 2, {"idA": "entrez gene:374918"}),
            (1, 2, {"idsA": "uniprotkb:ERRFI_HUMAN"}),
            (1, 14, {"source": "biogrid", "conf": {"$gt": 0.92}})
        ]
        for maxdepth, n, qc in tests:
            agpl = [
                {'$match': qc},
                {"$graphLookup": {
                    "from": DOCTYPE,
                    "startWith": "$idB",
                    "connectToField":
                        "idA",
                    "connectFromField":
                        "idB",
                    "as": "neighbors",
                    "maxDepth": maxdepth,
                    "depthField": "depth"
                }},
                {"$unwind": "$neighbors"},
                {"$group": {
                    "_id": {"idsA": "$idsA", "depth": "$neighbors.depth"},
                    "neighbors": {"$addToSet": "$neighbors.idsB"}
                }}
            ]
            r = self.aggregate_query(agpl)
            neighbors = [c for c in r]
            assert len(neighbors) == n


if __name__ == '__main__':
    unittest.main()
