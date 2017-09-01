#!/usr/bin/env python
""" Sample queries with MetaNetX compounds and reactions """

import os
import unittest

from nosqlbiosets.dbutils import DBconnection


def esquery(es, index, qc, doc_type=None, size=0):
    print("querying '%s'  %s" % (doc_type, str(qc)))
    r = es.search(index=index, doc_type=doc_type,
                  body={"size": size, "query": qc})
    nhits = r['hits']['total']
    return r['hits']['hits'], nhits


class QueryMetanetx(unittest.TestCase):
    d = os.path.dirname(os.path.abspath(__file__))
    index = "pathdes"

    def query_keggids(self, dbc, cids):
        if dbc.db == 'Elasticsearch':
            index, doctype = "metanetx", "compound"
            qc = {"match": {"xrefs.id": ' '.join(cids)}}
            hits, n = esquery(dbc.es, index, qc, doctype, len(cids))
            mids = [xref['_id'] for xref in hits]
        else:  # MongoDB
            doctype = "metanetx_compound"
            qc = {'xrefs.id': {'$in': cids}}
            hits = dbc.mdbi[doctype].find(qc, limit=10)
            mids = [c['_id'] for c in hits]
        print(mids)
        return mids

    def query_metanetxids(self, dbc, mids):
        doctype = "compound"
        if dbc.db == 'Elasticsearch':
            index = "metanetx"
            qc = {"ids": {"values": mids}}
            hits, n = esquery(dbc.es, index, qc, doctype, len(mids))
            descs = [c['_source']['desc'] for c in hits]
        else:  # MongoDB
            doctype = "metanetx_compound"
            qc = {"_id": {"$in": mids}}
            hits = dbc.mdbi[doctype].find(qc, limit=10)
            descs = [c['desc'] for c in hits]
        print(descs)
        return descs

    def id_queries(self, db):
        dbc = DBconnection(db, self.index)
        mids = self.query_keggids(dbc, ['C00116', 'C05433'])
        self.assertEqual(mids, ['MNXM2000', 'MNXM89612'])
        descs = self.query_metanetxids(dbc, mids)
        self.assertEqual(set(descs), {'glycerol', 'alpha-carotene'})

    # link metanetx.reaction.ecno to uniprot.dbreference.id
    #     where uniprot.dbreference.type = EC
    def test_ecno2gene(self, db='Elasticsearch'):
        doctype = "reaction"
        dbc = DBconnection(db, self.index)
        keggids = [('R01047', {'dhaB'}), ('R03119', {'dhaT'})]
        for keggid, genes in keggids:
            if db == "Elasticsearch":
                qc = {"match": {"xrefs.id": keggid}}
                hits, n = esquery(dbc.es, self.index, qc, doctype, 10)
                ecnos = [r['_source']['ecno'] for r in hits]
            else:
                doctype = "metanetx_reaction"
                qc = {"xrefs.id": keggid}
                hits = dbc.mdbi[doctype].find(qc, limit=10)
                ecnos = [r['ecno'] for r in hits]
            print(len(ecnos))
            for ecno in ecnos:
                for ecn in ecno.split(';'):
                    r = self.uniprot_ecno2genename_query(dbc, ecn)
                    self.assertSetEqual(genes, r)

    def test_ecno2gene_mdb(self):
        db = 'MongoDB'
        self.test_ecno2gene(db)

    @staticmethod
    def uniprot_ecno2genename_query(dbc, ecn):
        index, doctype = "pathdes", "protein"
        if dbc.db == "MongoDB":
            qc = {"dbReference.id": ecn}
            hits = dbc.mdbi[doctype].find(qc, limit=10)
            genes = [r['gene']['name'] for r in hits]
        else:
            qc = {"match": {"dbReference.id": ecn}}
            hits, n = esquery(dbc.es, index, qc, doctype, 10)
            genes = [r['_source']['gene']['name'] for r in hits]
        gnames = set()
        for gene in genes:
            if isinstance(gene, list):
                for gen in gene:
                    gnames.add(gen['#text'])
            else:
                gnames.add(gene['#text'])
        print(gnames)
        return gnames

    def test_id_queries_es(self):
        self.id_queries("Elasticsearch")

    def test_id_queries_mdb(self):
        self.id_queries("MongoDB")


if __name__ == '__main__':
    unittest.main()
