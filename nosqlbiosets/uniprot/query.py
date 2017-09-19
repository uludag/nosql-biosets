#!/usr/bin/env python
""" Simple queries with UniProt data indexed with MongoDB or Elasticsearch """

from ..dbutils import DBconnection


class QueryUniProt:

    def __init__(self):
        self.index = "biosets"
        self.doctype = "uniprot"
        self.dbc = DBconnection("MongoDB", self.index)

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

    # Get UniProt names(=ids) for given KEGG gene ids
    def getnamesforkegggeneids(self, kgids, db="MongoDB"):
        if db == 'Elasticsearch':
            esc = DBconnection(db, self.index)
            qc = {"match": {
                "dbReference.id": "(\"%s\")" % '" OR "'.join(kgids)}}
            hits, n, _ = self.esquery(esc.es, self.index, {"query": qc},
                                      self.doctype, len(kgids))
            r = [xref['_id'] for xref in hits]
        else:
            qc = {"dbReference.id": {'$in': kgids}}
            key = 'name'
            r = self.dbc.mdbi[self.doctype].distinct(key, filter=qc)
            print("#accs = %d" % len(r))
            print(qc)
        print(r)
        return r

    @staticmethod
    def esquery(es, index, qc, doc_type=None, size=0):
        print("Querying '%s'  %s" % (doc_type, str(qc)))
        r = es.search(index=index, doc_type=doc_type, body=qc, size=size)
        nhits = r['hits']['total']
        aggs = r["aggregations"] if "aggregations" in r else None
        return r['hits']['hits'], nhits, aggs
