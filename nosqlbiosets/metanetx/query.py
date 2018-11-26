#!/usr/bin/env python
""" Queries with MetaNetX data indexed with MongoDB or Elasticsearch """

import json
import re

import networkx as nx
from nosqlbiosets.dbutils import DBconnection
from nosqlbiosets.metanetx.index import TYPE_COMPOUND, TYPE_REACTION


def cobrababel_parse_metanetx_equation(equation):
    """ Note: This function is a copy of the _parse_metanetx_equation() function
              in cobrababel project metanetx.py file:
              https://github.com/mmundy42/cobrababel
              https://github.com/mmundy42/cobrababel/blob/master/LICENSE.txt
    Copyright (c) 2017,2017, Mayo Foundation for Medical Education and Research
    """
    metabolite_re = re.compile(
        r'(\d*\.\d+|\d+) (MNXM\d+|BIOMASS)@(MNXD[\dX]|BOUNDARY)')

    parts = equation.split(' = ')
    if len(parts) != 2:
        return None

    # Build a dictionary keyed by metabolite ID with the information needed for
    # setting the metabolites in a cobra.core.Reaction object.
    metabolites = dict()
    reactants = parts[0].split(' + ')
    for r in reactants:
        match = re.search(metabolite_re, r)
        if match is None:
            return None
        met_id = '{0}_{1}'.format(match.group(2), match.group(3))
        metabolites[met_id] = {
            'mnx_id': match.group(2),
            'coefficient': -1.0 * float(match.group(1)),
            'compartment': match.group(3)
        }
    products = parts[1].split(' + ')
    for p in products:
        match = re.search(metabolite_re, p)
        if match is None:
            return None
        met_id = '{0}_{1}'.format(match.group(2), match.group(3))
        metabolites[met_id] = {
            'mnx_id': match.group(2),
            'coefficient': float(match.group(1)),
            'compartment': match.group(3)
        }
    return metabolites


class QueryMetaNetX:

    def __init__(self, db="MongoDB"):
        self.index = "biosets"
        self.dbc = DBconnection(db, self.index)

    # Given MetaNetX compound id return its name
    def getcompoundname(self, dbc, mid, limit=0):
        if dbc.db == 'Elasticsearch':
            index, doctype = TYPE_COMPOUND, "_doc"
            qc = {"match": {"_id": mid}}
            hits, n = self.esquery(dbc.es, index, qc, doctype)
        else:  # MongoDB
            qc = {"_id": mid}
            hits = list(dbc.mdbi[TYPE_COMPOUND].find(qc, limit=limit))
            n = len(hits)
        assert 1 == n, "%s %s" % (dbc.db, mid)
        c = hits[0]
        desc = c['desc'] if 'desc' in c else c['_source']['desc']
        return desc

    def autocomplete_metabolitenames(self, qterm, **kwargs):
        """
        Given query term find possible metabolite names that match query term
        :param qterm: query term
        """
        qc = {"desc": {"$regex": "^%s" % qterm, "$options": "i"}}
        r = self.query_metabolites(qc, projection=['desc'], **kwargs)
        return list(r)

    # Text search metabolites with given query term
    def textsearch_metabolites(self, queryterm):
        assert "MongoDB" == self.dbc.db
        doctype = TYPE_COMPOUND
        qc = {'$text': {'$search': queryterm}}
        hits = self.dbc.mdbi[doctype].find(qc, projection={
            'score': {'$meta': "textScore"}})
        r = [c for c in hits]
        r.sort(key=lambda x: x['score'])
        return r

    # Given KEGG compound ids find ids for other libraries
    def keggcompoundids2otherids(self, dbc, cids, lib='MetanetX'):
        if dbc.db == 'Elasticsearch':
            index, doctype = TYPE_COMPOUND, "_doc"
            qc = {"match": {"xrefs.id": ' '.join(cids)}}
            hits, n = self.esquery(dbc.es, index, qc, doctype, len(cids))
        else:  # MongoDB
            qc = {'xrefs.id': {'$in': cids}}
            hits = list(dbc.mdbi[TYPE_COMPOUND].find(qc))
            n = len(hits)
        assert len(cids) == n
        mids = [None] * n
        for c in hits:
            i, libids = None, []
            for xref in (c['xrefs'] if 'xrefs' in c else c['_source']['xrefs']):
                if i is None and xref['lib'] == 'kegg':
                    i = cids.index(xref['id'][0])
                if xref['lib'] == lib:
                    if libids is []:
                        libids = xref['id']
                    else:
                        libids += xref['id']
            if i is not None:
                mids[i] = libids
        return mids

    @staticmethod
    def esquery(es, index, qc, doc_type=None, size=10):
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
        r = self.dbc.mdbi[TYPE_COMPOUND].find(qc, **kwargs)
        return r

    # Query metabolites with given query clause
    def aggregatequery_metabolites(self, qc, **kwargs):
        assert "MongoDB" == self.dbc.db
        assert qc is not None
        r = self.dbc.mdbi[TYPE_COMPOUND].aggregate(qc, **kwargs)
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
        if "MongoDB" == self.dbc.db:
            hits = self.dbc.mdbi[TYPE_REACTION].find(qc, **kwargs)
            r = [c for c in hits]
        else:
            index, doctype = TYPE_REACTION, "_doc"
            r, _ = self.esquery(self.dbc.es, index, qc, doctype, **kwargs)
        return r

    # Query reactions and return reactions together with their metabolites
    def reactionswithmetabolites(self, qc, **kwargs):
        cr = self.dbc.mdbi[TYPE_REACTION].find(qc, **kwargs)
        reacts = []
        mids = set()
        for r in cr:
            eq = cobrababel_parse_metanetx_equation(r['equation'])
            if eq is None:
                continue
            r['reactants'] = set()
            r['products'] = set()
            for m in eq.items():
                mid = m[1]['mnx_id']
                mids.add(mid)
                if m[1]['coefficient'] < 0:
                    r['reactants'].add(mid)
                else:
                    r['products'].add(mid)
            reacts.append(r)
        qc = {"_id": {"$in": list(mids)}}
        cr = self.query_metabolites(qc, projection=['desc'])
        # TODO: option to return metabolite names based on selected library
        metabolites = {i['_id']: i['desc'] for i in cr}
        return reacts, metabolites

    def get_metabolite_network(self, qc):
        """
        Get network of metabolites,
        edges refer to the unique set of reactions connecting two metabolites
        :param qc: specify the subset of reactions
        :return: metabolites graph as networkX object
        """
        reacts, metabolites = self.reactionswithmetabolites(qc)

        mn = nx.DiGraph(name='metanetx',
                        query=json.dumps(qc).replace('"', '\''))
        for r in reacts:
            for u_ in r['reactants']:
                u = metabolites[u_]
                for v in r['products']:
                    v = metabolites[v]
                    if mn.has_edge(u, v):
                        er = mn.get_edge_data(u, v)['reactions']
                        er.append(r['_id'])
                    else:
                        er = [r['_id']]
                        mn.add_edge(u, v, reactions=er,
                                    source=r['source']['lib'], ec=r['ecno'])

        return mn
