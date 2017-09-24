#!/usr/bin/env python
""" Simple queries with MetaNetX data indexed with MongoDB or Elasticsearch """

from ..dbutils import DBconnection


class QueryMetaNetX:

    def __init__(self):
        self.index = "biosets"
        self.dbc = DBconnection("MongoDB", self.index)

    # Query 'compound's with MetaNetX ids return descs
    def query_metanetxids(self, dbc, mids):
        if dbc.db == 'Elasticsearch':
            index, doctype = "metanetx", "metanetx_compound"
            qc = {"ids": {"values": mids}}
            hits, n = self.esquery(dbc.es, index, qc, doctype, len(mids))
            descs = [c['_source']['desc'] for c in hits]
        else:  # MongoDB
            doctype = "metanetx_compound"
            qc = {"_id": {"$in": mids}}
            hits = dbc.mdbi[doctype].find(qc, limit=10)
            descs = [c['desc'] for c in hits]
        print(descs)
        return descs

    # Given KEGG compound ids find MetaNetX ids
    def keggcompoundids2metanetxids(self, dbc, cids):
        if dbc.db == 'Elasticsearch':
            index, doctype = "metanetx", "metanetx_compound"
            qc = {"match": {"xrefs.id": ' '.join(cids)}}
            hits, n = self.esquery(dbc.es, index, qc, doctype, len(cids))
            mids = [xref['_id'] for xref in hits]
        else:  # MongoDB
            doctype = "metanetx_compound"
            qc = {'xrefs.id': {'$in': cids}}
            hits = dbc.mdbi[doctype].find(qc, limit=10)
            mids = [c['_id'] for c in hits]
        print(mids)
        return mids

    @staticmethod
    def esquery(es, index, qc, doc_type=None, size=0):
        print("Querying '%s'  %s" % (doc_type, str(qc)))
        r = es.search(index=index, doc_type=doc_type,
                      body={"query": qc}, size=size)
        nhits = r['hits']['total']
        return r['hits']['hits'], nhits
