#!/usr/bin/env python
""" Simple queries with UniProt data indexed with MongoDB or Elasticsearch """

import json
import unittest

from nosqlbiosets.dbutils import DBconnection
from nosqlbiosets.uniprot.query import QueryUniProt

qryuniprot = QueryUniProt()


class TestQueryUniProt(unittest.TestCase):
    index = "biosets"
    doctype = "uniprot"

    def test_keggid_queries(self, db="Elasticsearch"):
        mids = qryuniprot.getnamesforkegggeneids(['hsa:7157', 'hsa:121504'], db)
        self.assertSetEqual(set(mids), {'P53_HUMAN', 'H4_HUMAN'})

    def test_keggid_queries_mdb(self):
        self.test_keggid_queries("MongoDB")

    def slow_aggs(self, db):
        dbc = DBconnection(db, self.index)
        doctype = "uniprot"
        aggfl = "$comment.type"
        if db == "MongoDB":
            agpl = [
                {"$match": {"organism.name.#text": "Human"}},
                {"$unwind": aggfl},
                {"$group": {"_id": aggfl,
                            "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": 10}
            ]
            hits = dbc.mdbi[doctype].aggregate(agpl)
            docs = [i for i in hits]
        else:  # "Elasticsearch"
            qc = {
                "_source": "*.type",
                "query": {
                    "match": {
                        "organism.dbReference.id": "9606"}},
                "aggs": {
                    "tids": {
                        "terms": {
                            "field": "comment.type.keyword",
                            "size": 10}}}
            }
            _, _, docs = QueryUniProt.esquery(dbc.es, self.index, qc, doctype)
        print(json.dumps(docs, indent=2))

    def test_enzymesWithMostInteractions(self, db="MongoDB"):
        dbc = DBconnection(db, self.index)
        doctype = "uniprot"
        if db == "MongoDB":
            agpl = [
                {"$match": {
                    "organism.name.#text": "Baker's yeast",
                    "dbReference.type": "EC"}},
                {"$project": {"comment": 1}},
                {"$unwind": "$comment"},
                {"$match": {
                    "comment.interactant": {"$exists": True}}},
                {"$limit": 4}
            ]
            hits = dbc.mdbi[doctype].aggregate(agpl)
            docs = [i for i in hits]
            print(json.dumps(docs, indent=2))

    def test_aggs(self, db="MongoDB"):
        dbc = DBconnection(db, self.index)
        doctype = "uniprot"
        if db == "MongoDB":
            agpl = [
                {"$match": {
                    "organism.name.#text": "Baker's yeast",
                    "dbReference.type": "EC"}},
                {"$project": {"gene": 1, "dbReference": 1}},
                {"$unwind": "$dbReference"},
                {"$match": {
                    "dbReference.type": "EC"}},
                {"$unwind": "$gene.name.#text"},
                {"$group": {"_id": "$dbReference.id",
                            "genes": {"$addToSet": "$gene.name.#text"},
                            "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": 4},
            ]
            hits = dbc.mdbi[doctype].aggregate(agpl)
            docs = [i for i in hits]
        else:  # Elasticsearch
            # Not equivalent to above MongoDB query,   incomplete work
            qc = {
                # "_source": "*.type",
                "size": 0,
                "query": {
                    "query_string": {
                        "query":
                        "organism.name.#text.keyword:Baker's yeast AND "
                        "dbReference.type.keyword:EC"}},
                "aggs": {
                    "tids": {
                        "terms": {
                            "field": "dbReference.id.keyword",
                            "size": 10}}}
            }
            _, _, docs = QueryUniProt.esquery(dbc.es, self.index, qc, doctype)
        print(json.dumps(docs, indent=2))

    def test_lookup(self, db="MongoDB"):
        dbc = DBconnection(db, self.index)
        doctype = "metanetx_reaction"
        keggids = [('R01047', {'dhaB'}), ('R03119', {'dhaT'})]
        if db == "MongoDB":
            for keggid, genes in keggids:
                agpl = [
                    {"$match": {"xrefs.id": keggid}},
                    {"$project": {"ecno": 1}},
                    {"$lookup": {
                        "from": "uniprot",
                        "localField": "ecno",
                        "foreignField": "dbReference.id",
                        "as": "uniprot"
                    }},
                    {"$unwind": "$uniprot"},
                    {"$project": {"ecno": 1, "uniprot.gene.name.#text": 1}},
                ]
                hits = dbc.mdbi[doctype].aggregate(agpl)
                docs = [i for i in hits]
                # print(json.dumps(docs, indent=2))
                self.assertSetEqual(genes,
                                    {doc['uniprot']['gene']['name']['#text']
                                     for doc in docs})

    def test_getenzymedata(self):
        enzys = [('2.2.1.11', {'Q58980'}, {'MJ1585'})]
        for ecn, accs, genes in enzys:
            self.assertSetEqual(genes, set(qryuniprot.getgenes(ecn)))
            self.assertSetEqual(accs, set(qryuniprot.getaccs(ecn)))
            self.assertIn("Aromatic compound metabolism.",
                          qryuniprot.getpathways(ecn))


if __name__ == '__main__':
    unittest.main()
