#!/usr/bin/env python
""" Queries with MetaNetX data indexed with MongoDB or Elasticsearch """

import json
import re

import networkx as nx

from nosqlbiosets.dbutils import DBconnection
from nosqlbiosets.graphutils import remove_highly_connected_nodes
from nosqlbiosets.metanetx.index import TYPE_COMPOUND, TYPE_REACTION
from nosqlbiosets.qryutils import parseinputquery


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

    def __init__(self, db="MongoDB", index='biosets', version="", **kwargs):
        self.dbc = DBconnection(db, index, **kwargs)
        self.rcollection = TYPE_REACTION+version
        self.ccollection = TYPE_COMPOUND+version

    # Given MetaNetX compound id return its name
    def getcompoundname(self, mid, limit=0):
        if self.dbc.db == 'Elasticsearch':
            index, doctype = TYPE_COMPOUND, "_doc"
            qc = {"match": {"_id": mid}}
            hits, n = self.esquery(self.dbc.es, index, qc, doctype)
        else:  # MongoDB
            qc = {"_id": mid}
            hits = list(self.dbc.mdbi[self.ccollection].find(qc, limit=limit))
            n = len(hits)
        assert 1 == n, "%s %s" % (self.dbc.db, mid)
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
        qc = {'$text': {'$search': queryterm}}
        hits = self.dbc.mdbi[self.ccollection].find(qc, projection={
            'score': {'$meta': "textScore"}})
        r = [c for c in hits]
        r.sort(key=lambda x: x['score'])
        return r

    # Given KEGG compound ids find ids for other libraries
    def keggcompoundids2otherids(self, cids, lib='MetanetX'):
        if self.dbc.db == 'Elasticsearch':
            index, doctype = TYPE_COMPOUND, "_doc"
            qc = {"match": {"xrefs.id": ' '.join(cids)}}
            hits, n = self.esquery(self.dbc.es, index, qc, doctype, len(cids))
        else:  # MongoDB
            qc = {'xrefs.id': {'$in': cids}}
            hits = list(self.dbc.mdbi[self.ccollection].find(qc))
            n = len(hits)
        assert len(cids) == n
        mids = [None] * n
        for c in hits:
            i, libids = None, []
            for xref in (c['xrefs'] if 'xrefs' in c else c['_source']['xrefs']):
                if i is None and xref['lib'] == 'kegg':
                    for id_ in xref['id']:
                        if id_ in cids:
                            i = cids.index(id_)
                            break
                if xref['lib'] == lib:
                    if libids is []:
                        libids = xref['id']
                    else:
                        libids += xref['id']
            if i is not None:
                assert len(libids) >= 1
                mids[i] = libids[0]  # TODO: check me
        return mids

    @staticmethod
    def esquery(es, index, qc, doc_type=None, size=10):
        print("Querying '%s'  %s" % (doc_type, str(qc)))
        r = es.search(index=index,
                      body={"query": qc}, size=size)
        nhits = r['hits']['total']
        return r['hits']['hits'], nhits

    # Query metabolites with given query clause
    def query_metabolites(self, qc, **kwargs):
        assert "MongoDB" == self.dbc.db
        if qc is None:
            qc = {}
        r = self.dbc.mdbi[self.ccollection].find(qc, **kwargs)
        return r

    # Query metabolites with given query clause
    def aggregatequery_metabolites(self, qc, **kwargs):
        assert "MongoDB" == self.dbc.db
        assert qc is not None
        r = self.dbc.mdbi[self.ccollection].aggregate(qc, **kwargs)
        return r

    # Query metabolites with given query clause
    def aggregatequery_reactions(self, qc, **kwargs):
        assert "MongoDB" == self.dbc.db
        assert qc is not None
        r = self.dbc.mdbi[self.rcollection].aggregate(qc, **kwargs)
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
            hits = self.dbc.mdbi[self.rcollection].find(qc, **kwargs)
            r = [c for c in hits]
        else:
            index, doctype = self.rcollection, "_doc"
            r, _ = self.esquery(self.dbc.es, index, qc, doctype, **kwargs)
        return r

    # Query reactions and return reactions together with their metabolites
    def reactionswithmetabolites(self, qc, **kwargs):
        cr = self.dbc.mdbi[self.rcollection].find(qc, **kwargs)
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

    def get_metabolite_network(self, qc, sidec=None, selfloops=False,
                               max_degree=40):
        """
        Get network of metabolites,
        edges refer to the unique set of reactions connecting two metabolites
        :param qc: specify the subset of reactions in MetaNetX
        :param sidec: metabolites not to include in the graph returned
        :param selfloops: include self loops,
           metabolites are often on both sides of reactions
        :param max_degree: nodes with connections more than max_degree
           are not included in the result graph
        :return: metabolites graph as NetworkX object
        """
        reacts, metabolites = self.reactionswithmetabolites(qc)
        if sidec is not None:
            sidec = " ".join(sidec)
        mn = nx.DiGraph(name='MetaNetX',
                        query=json.dumps(qc).replace('"', '\''))
        for r in reacts:
            for u_ in r['reactants']:
                if sidec is not None and u_ in sidec:
                    continue
                u = metabolites[u_]
                for v in r['products']:
                    if not selfloops and v == u_:
                        continue
                    if sidec is not None and v in sidec:
                        continue
                    v = metabolites[v]
                    if mn.has_edge(u, v):
                        er = mn.get_edge_data(u, v)['reactions']
                        if r['_id'] not in er:
                            er.append(r['_id'])
                    else:
                        er = list([r['_id']])
                        mn.add_edge(u, v, reactions=er,
                                    sourcelib=r['source']['lib'], ec=r['ecno'])
        remove_highly_connected_nodes(mn, max_degree=max_degree)
        return mn


def cyview(query):
    """ See metabolite networks with Cytoscape runing on your local machine """
    from py2cytoscape.data.cyrest_client import CyRestClient
    qc = parseinputquery(query)
    qry = QueryMetaNetX()
    mn = qry.get_metabolite_network(qc, max_degree=maxdegree)
    client = CyRestClient()
    client.network.create_from_networkx(mn)


if __name__ == '__main__':
    import argh
    argh.dispatch_commands([
        cyview
    ])
