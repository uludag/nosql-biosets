#!/usr/bin/env python
""" Query UniProt data indexed with MongoDB or Elasticsearch """
# Server connection details are read from conf/dbservers.json file

from collections import OrderedDict

from nosqlbiosets.qryutils import Query


class QueryUniProt(Query):

    # Get UniProt acc ids for given enzyme
    def getaccs(self, ecn, reftype="EC"):
        qc = {"dbReference.id": ecn, "dbReference.type": reftype}
        key = 'accession'
        r = self.dbc.mdbi[self.mdbcollection].distinct(key, filter=qc)
        return r

    # Get names and abundance of the genes for given enzyme
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
            hits, n, aggs = self.esquery(self.index, qc)
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

    def getgeneids(self, qc, limit=1000):
        """ Given query return matching genes primary name and Entrez ids """
        assert self.dbc.db == 'MongoDB'
        aggq = [
            {"$match": qc},
            {"$project": {"dbReference": 1, "gene": 1}},
            {"$unwind": "$dbReference"},
            {"$match": {"dbReference.type": "GeneID"}},
            {"$project": {
                "gid": "$dbReference.id", "gene": 1}},
            {"$unwind": "$gene"},
            {"$unwind": "$gene.name"},
            {"$match": {"gene.name.type": "primary"}},
            {"$project": {
                "gid": 1, "gene": "$gene.name.#text"}},
            {"$limit": limit}
        ]
        cr = self.aggregate_query(aggq)
        r = set()
        for i in cr:
            r.add((i['_id'], int(i['gid']), i['gene']))
        return r

    # Find abundance of annotations for the set specified by the query clause
    def getannotations(self, qc, annottype="GO"):
        assert self.dbc.db == 'MongoDB'
        aggq = [
            qc,
            {"$unwind": "$dbReference"},
            {"$match": {"dbReference.type": annottype}},
            {'$group': {
                '_id': {
                    'id': '$dbReference.id',
                    'name': {"$arrayElemAt": ['$dbReference.property', 0]}
                },
                "abundance": {"$sum": 1}
            }},
            {'$project': {
                "abundance": 1,
                "id": "$_id.id",
                "name": "$_id.name.value",
                "_id": 0
            }},
            {"$sort": {"abundance": -1}}
        ]
        r = self.aggregate_query(aggq)
        return r

    # Find related genes for given KEGG reaction id
    # UniProt ids are found by querying the IntEnz dataset with given KEGG id
    def genes_linkedto_keggreaction(self, keggrid):
        doctype = "intenz"
        if self.dbc.db == 'MongoDB':
            agpl = [
                {"$match": {"reactions.map.link.title": keggrid}},
                {"$unwind": "$links"},
                {"$match": {"links.db": "UniProt"}},
                {"$lookup": {
                    "from": self.mdbcollection,
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
            r = {doc['uniprot']['gene']['name']['#text']
                 for doc in docs}
            return r

    # Get names and observation numbers of the organisms for given enzyme
    # or for entries selected by the query clause qc
    def getorganisms(self, ecn, qc=None, limit=1000):
        if qc is None:
            assert ecn is not None
            qc = {"dbReference.id": ecn}
        if self.dbc.db == 'Elasticsearch':
            qc = {"query": {"match": qc},
                  "_source": "organism.name"
                  }
            hits, n, _ = self.esquery(self.index, qc, size=limit)
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
                        "name": "$organism.name.#text",
                        "taxon": "$organism.linage.taxon",
                    },
                    "total": {
                        "$sum": 1
                    }
                }},
                {"$project": {"type": "$_id.type", "name": "$_id.name",
                              "taxon": "$_id.taxon", "_id": 0, "total": 1}},
                {"$sort": {"total": -1}},
                {"$limit": limit}
            ]
            r = self.aggregate_query(aggq)
            rr = dict()
            for i in r:
                nametype = i['type']
                if nametype not in rr:
                    rr[nametype] = OrderedDict()
                rr[nametype][i['name']] = i['total']
        return rr

    def getspecies(self, qc):
        aggq = [
            {"$match": qc},
            {"$match": {"organism.lineage.taxon.0": {"$exists": True}}},
            {"$group": {
                "_id": {"$arrayElemAt": ['$organism.lineage.taxon', -1]}
            }}
        ]
        r = self.aggregate_query(aggq)
        r = [i['_id'] for i in r]
        return r

    def get_lca(self, qc):
        """
        Get lowest common ancestor for entries selected by the query clause qc
        """
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
                for s in lca:
                    if s != i['taxon'][j]:
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
                "_id": "$comment.reaction.text", "total": {"$sum": 1}}},
            {"$sort": {"total": -1}},
            {"$limit": limit}
        ]
        r = self.dbc.mdbi[self.mdbcollection].aggregate(aggq)
        return r

    # Get UniProt names(=ids) for given KEGG gene ids
    def getnamesforkegg_geneids(self, kgids, db="MongoDB"):
        if db == 'Elasticsearch':
            # esc = DBconnection(db, self.index)
            qc = {"terms": {
                "dbReference.id.keyword": kgids}}
            hits, _, _ = self.esquery(self.mdbcollection, {"query": qc})
            r = [xref['_id'] for xref in hits]
        else:
            qc = {"dbReference.id": {'$in': kgids}}
            key = 'name'
            r = self.dbc.mdbi[self.mdbcollection].distinct(key, filter=qc)
        return r

    def top_annotation_pairs(self, qc, limit=10):
        """ Return most abundant GO and Pfam annotations co-occurences
        qc: Query clause to select subsets of UniProt data
        """
        agpl = [
            {"$match": qc},
            {"$project": {
                "dbReference": 1
            }},
            {'$match': {
                'dbReference': {
                    '$type': 'array'}}},
            {'$group': {
                "_id": {
                    "go": {
                        "$filter": {
                            "input": "$dbReference",
                            "as": "r",
                            "cond": {
                                "$eq": ["$$r.type", "GO"]}
                        }},
                    "pfam": {
                        "$filter": {
                            "input": "$dbReference",
                            "as": "r",
                            "cond": {
                                "$eq": ["$$r.type", "Pfam"]}
                        }}
                },
                "abundance": {"$sum": 1}
            }},
            {"$unwind": "$_id.go"},
            {"$unwind": "$_id.pfam"},
            {"$sort": {"abundance": -1}},
            {"$limit": limit},
        ]
        r = self.aggregate_query(agpl, allowDiskUse=True)
        return r


def idmatch(idlist, limit=100, mdbdb="biosets", mdbcollection="uniprot", **kwargs):
    """ Given mixed protein/gene ids return Entrez id and primary gene name
    for each matching UniProt record """
    qry = QueryUniProt("MongoDB", mdbdb, mdbcollection, **kwargs)
    import sys
    if idlist == '-':
        ids = []
        for line in sys.stdin:
            ids.append(line.strip())
    else:
        ids = idlist.split(", ")
    qc = {
        "organism.dbReference.id": "9606",
        "$or": [
            {'name': {"$in": ids}},
            {'gene.name.#text': {"$in": ids}},
            {"dbReference": {'$elemMatch': {
                "id": {"$in": ids},
                "type": "GeneID"}}}
        ]
    }
    r = qry.getgeneids(qc, limit)
    if idlist == '-':
        for i in r:
            print("%s, %d, %s" % (i[0], i[1], i[2]))
    else:
        return r


if __name__ == '__main__':
    import argh
    argh.dispatch_commands([
        idmatch
    ])
