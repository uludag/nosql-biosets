#!/usr/bin/env python
""" Queries with UniProt data indexed with MongoDB or Elasticsearch """
# Server connection details are read from conf/dbserver.json file

from nosqlbiosets.dbutils import DBconnection


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
    def getaccs(self, ecn):
        qc = {"dbReference.id": ecn}
        key = 'accession'
        r = self.dbc.mdbi[self.doctype].distinct(key, filter=qc)
        return r

    # Get names of the genes for given enzyme
    def getgenes(self, ecn, limit=100):
        if self.dbc.db == 'Elasticsearch':
            qc = {"query": {"match": {"dbReference.id": ecn}},
                  "aggs": {
                      "genes": {
                          "terms": {
                              "field": "gene.name.#text.keyword",
                              "size": limit
                          }}}}
            hits, n, aggs = self.esquery(
                self.dbc.es, self.index, qc, self.doctype)
            r = (b['key'] for b in aggs['genes']['buckets'])
        else:
            qc = {"dbReference.id": ecn}
            key = 'gene.name.#text'
            r = self.dbc.mdbi[self.doctype].distinct(key, filter=qc,
                                                     limit=limit)
        return r

    # Find related genes for given KEGG reaction
    # Finds UniProt ids by querying the IntEnz dataset
    def genes_linkedto_keggreaction(self, keggrid):
        doctype = "intenz"
        if self.dbc.db == 'MongoDB':
            agpl = [
                {"$match": {"reactions.reaction.map.link.title": keggrid}},
                {"$unwind": "$links.link"},
                {"$match": {"links.link.db": "UniProt"}},
                {"$lookup": {
                    "from": self.doctype,
                    "localField": "links.link.accession_number",
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

    # Get names of the organisms for given enzyme
    def getorganisms(self, ecn, limit=100):
        if self.dbc.db == 'Elasticsearch':
            qc = {"query": {"match": {"dbReference.id": ecn}},
                  "aggs": {
                      "organisms": {
                          "terms": {
                              "field": "organism.name.#text.keyword",
                              "size": 100
                          }}}}
            hits, n, aggs = self.esquery(
                self.dbc.es, self.index, qc, self.doctype)
            r = (b['key'] for b in aggs['organisms']['buckets'])
        else:
            qc = {"dbReference.id": ecn}
            key = 'gene.name.#text'
            r = self.dbc.mdbi[self.doctype].distinct(key, filter=qc,
                                                     limit=limit)
        return r

    # Get names of the metabolic pathway(s) associated with an enzyme
    # http://www.uniprot.org/help/pathway
    def getpathways(self, ecn):
        aggq = [
            {"$match": {"dbReference.id": ecn}},
            {"$unwind": "$comment"},
            {"$match": {"comment.type": "pathway"}},
            {"$group": {"_id": "$comment.text.#text"}}
        ]
        r = self.dbc.mdbi[self.doctype].aggregate(aggq)
        r = [pathway['_id'] for pathway in r]
        return r

    # Catalytic activity of an enzyme, i.e. the chemical reaction it catalyzes
    # http://www.uniprot.org/help/catalytic_activity
    def getcatalyticactivity(self, ecn):
        aggq = [
            {"$match": {"dbReference.id": ecn}},
            {"$unwind": "$comment"},
            {"$match": {"comment.type": "catalytic activity"}},
            {"$group": {"_id": "$comment.text.#text"}}
        ]
        r = self.dbc.mdbi[self.doctype].aggregate(aggq)
        r = [pathway['_id'] for pathway in r]
        return r

    # Get UniProt names(=ids) for given KEGG gene ids
    def getnamesforkegggeneids(self, kgids, db="MongoDB"):
        if db == 'Elasticsearch':
            esc = DBconnection(db, self.index)
            qc = {"match": {
                "dbReference.id": "(%s)" % ' OR '.join(kgids)}}
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
        import json
        print("Querying '%s'  %s" % (doc_type, json.dumps(qc, indent=4)))
        r = es.search(index=index, doc_type=doc_type, body=qc, size=size)
        nhits = r['hits']['total']
        aggs = r["aggregations"] if "aggregations" in r else None
        return r['hits']['hits'], nhits, aggs
