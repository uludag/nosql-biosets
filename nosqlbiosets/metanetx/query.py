#!/usr/bin/env python
""" Queries with MetaNetX data indexed with MongoDB or Elasticsearch """


from nosqlbiosets.dbutils import DBconnection
from cobra import Model, Metabolite, Reaction, DictList
import re
import json

# Regular expression for metabolite compartments in reaction equations
COMPARTEMENT_RE = re.compile(r'@(MNXD[\d]|BOUNDARY)')
DOCTYPE = "metanetx_compound"


class QueryMetaNetX:

    def __init__(self):
        self.index = "biosets"
        self.dbc = DBconnection("MongoDB", self.index)

    # Given MetaNetX compound id return its name
    def getcompoundname(self, dbc, mid, limit=0):
        if dbc.db == 'Elasticsearch':
            index, doctype = "metanetx", DOCTYPE
            qc = {"match": {"_id": mid}}
            hits, n = self.esquery(dbc.es, index, qc, doctype)
        else:  # MongoDB
            doctype = DOCTYPE
            qc = {"_id": mid}
            hits = list(dbc.mdbi[doctype].find(qc, limit=limit))
            n = len(hits)
        assert 1 == n
        c = hits[0]
        desc = c['desc'] if 'desc' in c else c['_source']['desc']
        return desc

    # Given KEGG compound ids find ids for other libraries
    def keggcompoundids2otherids(self, dbc, cids, lib='MetanetX'):
        if dbc.db == 'Elasticsearch':
            index, doctype = "metanetx", DOCTYPE
            qc = {"match": {"xrefs.id": ' '.join(cids)}}
            hits, n = self.esquery(dbc.es, index, qc, doctype, len(cids))
        else:  # MongoDB
            doctype = DOCTYPE
            qc = {'xrefs.id': {'$in': cids}}
            hits = list(dbc.mdbi[doctype].find(qc))
            n = len(hits)
        assert len(cids) == n
        mids = [None] * n
        for c in hits:
            i, id_ = None, None
            for xref in (c['xrefs'] if 'xrefs' in c else c['_source']['xrefs']):
                if i is None and xref['lib'] == 'kegg' and xref['id'][0] == 'C':
                    i = cids.index(xref['id'])
                    if id_ is not None:
                        break
                if id_ is None and xref['lib'] == lib:
                    id_ = xref['id']
                    if i is not None:
                        break
            mids[i] = id_
        return mids

    @staticmethod
    def esquery(es, index, qc, doc_type=None, size=0):
        print("Querying '%s'  %s" % (doc_type, str(qc)))
        r = es.search(index=index, doc_type=doc_type,
                      body={"query": qc}, size=size)
        nhits = r['hits']['total']
        return r['hits']['hits'], nhits

    # Query metabolites with given query clause
    def query_metabolites(self, qc, **kwargs):
        assert "MongoDB" == self.dbc.db
        if qc is None:
            qc = {}
        doctype = DOCTYPE
        hits = self.dbc.mdbi[doctype].find(qc, **kwargs)
        r = [c for c in hits]
        return r

    # Query compartments with given query clause
    def query_compartments(self, qc=None):
        assert "MongoDB" == self.dbc.db
        if qc is None:
            qc = {}
        doctype = "metanetx_compartment"
        hits = self.dbc.mdbi[doctype].find(qc)
        r = [c for c in hits]
        return r

    # Query reactions with given query clause
    def query_reactions(self, qc, **kwargs):
        assert "MongoDB" == self.dbc.db
        doctype = "metanetx_reaction"
        hits = self.dbc.mdbi[doctype].find(qc, **kwargs)
        r = [c for c in hits]
        return r

    # Query reactions and return reactions together with their metabolites
    def universalmodel_reactionsandmetabolites(self, qc):
        from cobrababel.metanetx import _parse_metanetx_equation
        doctype = "metanetx_reaction"
        hits = self.dbc.mdbi[doctype].find(qc)
        reacts = [c for c in hits]
        mids = set()
        for r in reacts:
            eq = _parse_metanetx_equation(r['equation'])
            if eq is None:
                continue
            for m in eq.items():
                mid = m[1]['mnx_id']
                mids.add(mid)
        qc = {"_id": {"$in": list(mids)}}
        metabolites = self.query_metabolites(qc)
        return reacts, metabolites

    # Construct universal metabolic models with subset of reactions
    # specified by the query clause 'qc'
    def universal_model(self, qc):
        reacts, metabolites_ = self.universalmodel_reactionsandmetabolites(qc)
        metabolites = DictList()
        for m in metabolites_:
            metabolite = Metabolite(id=m['_id'],
                                    name=m['desc'],
                                    formula=m['formula'])
            metabolites.append(metabolite)

        um = Model('metanetx_universal')
        um.notes['source'] = 'MetaNetX %s' % json.dumps(qc).replace('"', '\'')
        um.add_metabolites(metabolites)
        for r in reacts:
            reaction = Reaction(id=r['_id'], name=r['_id'],
                                lower_bound=-1000.0,
                                upper_bound=1000.0)
            um.add_reactions([reaction])

            # COBRApy compartment_finder doesn't recognize MetaNetX compartments
            eq = r['equation']
            if eq.find('n') == -1:
                eq = COMPARTEMENT_RE.sub("", eq)
                reaction.build_reaction_from_string(eq,
                                                    reversible_arrow='=',
                                                    verbose=True)
        return um
