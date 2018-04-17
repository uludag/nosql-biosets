#!/usr/bin/env python
""" Query UniProt data indexed with MongoDB or Elasticsearch """
# Server connection details are read from conf/dbservers.json file

from nosqlbiosets.dbutils import DBconnection
from collections import OrderedDict


class QueryUniProt:

    def __init__(self, db, index, doctype):
        self.index = index
        self.doctype = doctype
        self.dbc = DBconnection(db, self.index)

    def query(self, qc, projection=None, limit=0):
        c = self.dbc.mdbi[self.doctype].find(qc, projection=projection,
                                             limit=limit)
        return c

    def aggregate_query(self, agpl):
        r = self.dbc.mdbi[self.doctype].aggregate(agpl)
        return r

    # Get UniProt acc ids for given enzyme
    def getaccs(self, ecn, reftype="EC"):
        qc = {"dbReference.id": ecn, "dbReference.type": reftype}
        key = 'accession'
        r = self.dbc.mdbi[self.doctype].distinct(key, filter=qc)
        return r

    # Get names and observation numbers of the genes for given enzyme
    # or for entries selected by the query clause qc
    def getgenes(self, ecn, qc=None, limit=100):
        if qc is None:
            qc = {"dbReference.id": ecn}
        if self.dbc.db == 'Elasticsearch':
            qc = {
                "query": {"match": qc},
                "aggs": {
                    "genes": {
                        "terms": {
                            "field": "gene.name.type.keyword",
                        },
                        "aggs": {
                            "name": {
                                "terms": {
                                    "field": "gene.name.#text.keyword",
                                    "size": limit
                                }}}}}}
            hits, n, aggs = self.esquery(
                self.dbc.es, self.index, qc, self.doctype)
            r = dict()
            for i in aggs['genes']['buckets']:
                nametype = i['key']
                r[nametype] = OrderedDict()
                for j in i['name']['buckets']:
                    r[nametype][j['key']] = j['doc_count']
        else:
            aggq = [
                {"$match": qc},
                {"$project": {'gene.name': 1}},
                {"$unwind": "$gene"},
                {"$unwind": "$gene.name"},
                {"$group": {
                    "_id": {
                        "type": "$gene.name.type",
                        "name": "$gene.name.#text"
                    },
                    "total": {
                        "$sum": 1
                    }
                }},
                {"$sort": {"total": -1}},
                {"$limit": limit}
            ]
            cr = self.aggregate_query(aggq)
            r = dict()
            for i in cr:
                nametype = i['_id']['type']
                if nametype not in r:
                    r[nametype] = OrderedDict()
                r[nametype][i['_id']['name']] = i['total']
        return r

    # Find related genes for given KEGG reaction id
    # Finds UniProt ids by querying the IntEnz dataset with given KEGG ids
    def genes_linkedto_keggreaction(self, keggrid):
        doctype = "intenz"
        if self.dbc.db == 'MongoDB':
            agpl = [
                {"$match": {"reactions.map.link.title": keggrid}},
                {"$unwind": "$links"},
                {"$match": {"links.db": "UniProt"}},
                {"$lookup": {
                    "from": self.doctype,
                    "localField": "links.accession_number",
                    "foreignField": "accession",
                    "as": "uniprot"
                }},
                {"$unwind": "$uniprot"},
                {"$unwind": "$uniprot.gene"},
                {"$unwind": "$uniprot.gene.name"},
                {"$project": {"uniprot.gene.name.#text": 1}},
            ]
            docs = self.dbc.mdbi[doctype].aggregate(agpl)
            docs = list(docs)
            r = {doc['uniprot']['gene']['name']['#text']
                 for doc in docs}
            return r

    # Get names and observation numbers of the organisms for given enzyme
    # or for entries selected by the query clause qc
    def getorganisms(self, ecn, qc=None, limit=100):
        if qc is None:
            assert ecn is not None
            qc = {"dbReference.id": ecn}
        if self.dbc.db == 'Elasticsearch':
            qc = {"query": {"match": qc},
                  "_source": "organism.name"
                  }
            hits, n, _ = self.esquery(
                self.dbc.es, self.index, qc, self.doctype, size=limit)
            rr = dict()

            def digestnames(name):
                nametype_ = name['type']
                organism = name['#text']
                if nametype_ not in rr:
                    rr[nametype_] = OrderedDict()
                if organism in rr[nametype_]:
                    rr[nametype_][organism] += 1
                else:
                    rr[nametype_][organism] = 1

            for names in [hit['_source']['organism']['name'] for hit in hits]:
                if isinstance(names, list):
                    for name_ in names:
                        digestnames(name_)
                else:
                    digestnames(names)
        else:
            aggq = [
                {"$match": qc},
                {"$project": {'organism.name': 1}},
                {"$unwind": "$organism.name"},
                {"$group": {
                    "_id": {
                        "type": "$organism.name.type",
                        "name": "$organism.name.#text"
                    },
                    "total": {
                        "$sum": 1
                    }
                }},
                {"$sort": {"total": -1}},
                {"$limit": limit}
            ]
            r = self.aggregate_query(aggq)
            rr = dict()
            for i in r:
                nametype = i['_id']['type']
                if nametype not in rr:
                    rr[nametype] = OrderedDict()
                rr[nametype][i['_id']['name']] = i['total']
        return rr

    # Get lowest common ancestor for entries selected by the query clause qc
    def get_lca(self, qc):
        aggq = [
            {"$match": qc},
            {"$project": {'organism.lineage.taxon': 1}},
            {"$project": {'_id': 0, 'taxon': '$organism.lineage.taxon'}}
        ]
        r = self.aggregate_query(aggq)
        lca = None
        for i in r:
            if lca is None:
                lca = i['taxon']
            else:
                j = 0
                for taxon in lca:
                    if taxon != i['taxon'][j]:
                        lca = lca[:j]
                        break
                    else:
                        j += 1
                if j == 0:
                    break
        return lca

    # Get names of the metabolic pathway(s) associated with an enzyme,
    # or for entries selected by the query clause qc
    # http://www.uniprot.org/help/pathway
    def getpathways(self, ecn, qc=None, limit=100):
        if qc is None:
            qc = {"dbReference.id": ecn}
        aggq = [
            {"$match": qc},
            {"$unwind": "$comment"},
            {"$match": {"comment.type": "pathway"}},
            {"$group": {"_id": "$comment.text.#text", "total": {"$sum": 1}}},
            {"$sort": {"total": -1}},
            {"$limit": limit}
        ]
        r = self.aggregate_query(aggq)
        return r

    # Catalytic activities of an enzyme, or of entries selected
    # by the query clause qc
    # i.e. the chemical reactions catalyzed by enzyme(s)
    # http://www.uniprot.org/help/catalytic_activity
    def getcatalyticactivity(self, ecn, qc=None, limit=100):
        if qc is None:
            qc = {"dbReference.id": ecn}
        aggq = [
            {"$match": qc},
            {"$unwind": "$comment"},
            {"$match": {"comment.type": "catalytic activity"}},
            {"$group": {
                "_id": "$comment.text.#text", "total": {"$sum": 1}}},
            {"$sort": {"total": -1}},
            {"$limit": limit}
        ]
        r = self.dbc.mdbi[self.doctype].aggregate(aggq)
        return r

    # Get UniProt names(=ids) for given KEGG gene ids
    def getnamesforkegg_geneids(self, kgids, db="MongoDB"):
        if db == 'Elasticsearch':
            esc = DBconnection(db, self.index)
            qc = {"terms": {
                "dbReference.id.keyword": kgids}}
            hits, _, _ = self.esquery(esc.es, self.index, {"query": qc},
                                      self.doctype)
            r = [xref['_id'] for xref in hits]
        else:
            qc = {"dbReference.id": {'$in': kgids}}
            key = 'name'
            r = self.dbc.mdbi[self.doctype].distinct(key, filter=qc)
        return r

    @staticmethod
    def esquery(es, index, qc, doc_type=None, size=10):
        import json
        print("Querying '%s'  %s" % (doc_type, json.dumps(qc, indent=4)))
        r = es.search(index=index, doc_type=doc_type, body=qc, size=size)
        nhits = r['hits']['total']
        aggs = r["aggregations"] if "aggregations" in r else None
        return r['hits']['hits'], nhits, aggs
