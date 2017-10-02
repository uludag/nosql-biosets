#!/usr/bin/env python
""" Queries with MetaNetX data indexed with MongoDB or Elasticsearch """


from nosqlbiosets.dbutils import DBconnection
from cobra import Model, Metabolite, Reaction, DictList
import re
import json

# Regular expression for metabolite compartments in reaction equations
compartment_re = re.compile(r'@(MNXD[\d]|BOUNDARY)')


class QueryMetaNetX:

    def __init__(self):
        self.index = "biosets"
        self.dbc = DBconnection("MongoDB", self.index)

    # Query MetaNetX compounds with their ids return descs
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
        return mids

    @staticmethod
    def esquery(es, index, qc, doc_type=None, size=0):
        print("Querying '%s'  %s" % (doc_type, str(qc)))
        r = es.search(index=index, doc_type=doc_type,
                      body={"query": qc}, size=size)
        nhits = r['hits']['total']
        return r['hits']['hits'], nhits

    # Query metabolites with given query clause
    def query_metabolites(self, qc):
        if qc is None:
            qc = {}
        doctype = "metanetx_compound"
        hits = self.dbc.mdbi[doctype].find(qc)
        r = [c for c in hits]
        return r

    # Query compartments with given query clause
    def query_compartments(self, qc=None):
        if qc is None:
            qc = {}
        doctype = "metanetx_compartment"
        hits = self.dbc.mdbi[doctype].find(qc)
        r = [c for c in hits]
        return r

    # Query reactions with given query clause
    def query_reactions(self, qc):
        doctype = "metanetx_reaction"
        hits = self.dbc.mdbi[doctype].find(qc, limit=10)
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

    # Construct universal metabolic model with the reactions subset
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

            # cobrapy compartment_finder doesn't recognize MetaNetX compartments
            eq = r['equation']
            if eq.find('n') == -1:
                eq = compartment_re.sub("", eq)
                reaction.build_reaction_from_string(eq,
                                                    reversible_arrow='=',
                                                    verbose=True)
        return um
