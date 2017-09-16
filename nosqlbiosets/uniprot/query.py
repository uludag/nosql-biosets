#!/usr/bin/env python
""" Simple queries with UniProt data indexed with MongoDB """

from ..dbutils import DBconnection


class QueryUniProt:

    def __init__(self):
        index = "biosets"
        self.doctype = "uniprot"
        self.dbc = DBconnection("MongoDB", index)

    # Get UniProt acc ids for given enzyme
    def getaccs(self, ecn):
        qc = {"dbReference.id": ecn}
        key = 'accession'
        r = self.dbc.mdbi[self.doctype].distinct(key, filter=qc)
        print("#accs = %d" % len(r))
        return r

    # Get names of the genes for given enzyme
    def getgenes(self, ecn):
        qc = {"dbReference.id": ecn}
        key = 'gene.name.#text'
        r = self.dbc.mdbi[self.doctype].distinct(key, filter=qc)
        print("#accs = %d" % len(r))
        return r

    # Get names of the pathways for given enzyme
    def getpathways(self, ecn):
        qc = {"dbReference.id": ecn, "comment.type": "pathway"}
        # TODO: unwind comments and filter them with their type
        key = 'comment.text.#text'
        r = self.dbc.mdbi[self.doctype].distinct(key, filter=qc)
        print("#accs = %d" % len(r))
        return r