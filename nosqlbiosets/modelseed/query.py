#!/usr/bin/env python
""" Queries with ModelSEEDDatabase data indexed with MongoDB,
    few queries with Elasticsearch """
import json
import re

import networkx as nx

from nosqlbiosets.dbutils import DBconnection
from nosqlbiosets.qryutils import parseinputquery

# MongoDB collection names or Elasticsearch index names:
COMPOUNDSTYPE = "modelseed_compound"
REACTIONSTYPE = "modelseed_reaction"


def modelseeddb_parse_equation(equation, delimiter=' '):
    """
    This function is a copy of ModelSEEDDatabase Biochem_Helper.py:parseEquation
    https://github.com/ModelSEED/ModelSEEDDatabase/Scripts/Biochem_Helper.py
    """
    # Build search strings using specified delimiter.
    bidirectional = delimiter + '<=>' + delimiter
    reverse = delimiter + '<=' + delimiter
    forward = delimiter + '=>' + delimiter
    separator = delimiter + '+' + delimiter
    # Find the special string that separates reactants and products.
    reactants = list()
    products = list()
    if equation.find(forward) >= 0:
        direction = '>'
        parts = equation.split(forward)
        if parts[0]:
            reactants = parts[0].split(separator)
        if parts[1]:
            products = parts[1].split(separator)
    elif equation.find(reverse) >= 0:
        direction = '<'
        parts = equation.split(reverse)
        if parts[1]:
            reactants = parts[1].split(separator)
        if parts[0]:
            products = parts[0].split(separator)
    elif equation.find(bidirectional) >= 0:
        direction = '='
        parts = equation.split(bidirectional)
        if parts[0]:
            reactants = parts[0].split(separator)
        if parts[1]:
            products = parts[1].split(separator)
    else:
        return None, None, None

    return reactants, products, direction


class QueryModelSEED:

    def __init__(self, db="MongoDB", index='biosets', version="", **kwargs):
        self.dbc = DBconnection(db, index, **kwargs)
        self.rcollection = REACTIONSTYPE+version
        self.ccollection = COMPOUNDSTYPE+version

    # Given ModelSEED compound id return its name
    def getcompoundname(self, dbc, mid, limit=0):
        if dbc.db == 'Elasticsearch':
            index, doctype = COMPOUNDSTYPE, "_doc"
            qc = {"match": {"_id": mid}}
            hits, n = self.esquery(dbc.es, index, qc, doctype)
        else:  # MongoDB
            qc = {"_id": mid}
            hits = list(dbc.mdbi[self.ccollection].find(qc, limit=limit))
            n = len(hits)
        assert 1 == n
        c = hits[0]
        desc = c['name'] if 'name' in c else c['_source']['name']
        return desc

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
        r = self.dbc.mdbi[self.ccollection].find(qc, **kwargs)
        return r

    # Text search metabolites with given query term
    def textsearch_metabolites(self, queryterm):
        assert "MongoDB" == self.dbc.db
        qc = {'$text': {'$search': queryterm}}
        hits = self.dbc.mdbi[self.ccollection].find(qc, projection={
            'score': {'$meta': "textScore"}})
        r = [c for c in hits]
        r.sort(key=lambda x: x['score'])
        return r

    def autocomplete_metabolitenames(self, qterm, **kwargs):
        """
        Given query term find possible metabolite names that match query term
        :param qterm: query term
        """
        qc = {"$or": [
            {"name": {
                "$regex": "^%s" % qterm, "$options": "i"}},
            {"abbreviation": {
                "$regex": "^%s" % qterm, "$options": "i"}}
        ]}
        r = self.query_metabolites(qc, projection=['name'], **kwargs)
        return list(r)

    def get_metabolite_network(self, qc, **kwargs):
        """ Get graph of metabolites for given reaction query,
        edges data include the set of reactions connecting two metabolites
        """
        graph = nx.DiGraph(name='ModelSEEDdb', query=json.dumps(qc))
        reacts = self.dbc.mdbi[self.rcollection].\
            find(qc, projection=['name', 'equation'], **kwargs)
        mre = re.compile(r'\((\d*\.*\d*(e-\d+)?)\) (cpd\d+)\[(\d+)\]')
        r = self.query_metabolites({}, projection=['name'])
        id2name = {i['_id']: i['name'] for i in r}
        for r in reacts:
            reactants, products, _ = \
                modelseeddb_parse_equation(r['equation'])
            assert reactants is not None
            for u in reactants:
                match = re.search(mre, u)
                u = match.group(3)
                u = id2name[u]
                if not graph.has_node(u):
                    graph.add_node(u)
                for v in products:
                    match = re.search(mre, v)
                    v = match.group(3)
                    v = id2name[v]
                    if not graph.has_node(v):
                        graph.add_node(v)
                    if graph.has_edge(u, v):
                        er = graph.get_edge_data(u, v)['reactions']
                        if r['name'] not in er:
                            er.append(r['name'])
                    else:
                        er = list([r['name']])
                        graph.add_edge(u, v, reactions=er)
        return graph


def cyview(query):
    """ See metabolite networks with Cytoscape runing on your local machine """
    from py2cytoscape.data.cyrest_client import CyRestClient
    qc = parseinputquery(query)
    qry = QueryModelSEED()
    mn = qry.get_metabolite_network(qc)
    client = CyRestClient()
    client.network.create_from_networkx(mn)


if __name__ == '__main__':
    import argh
    argh.dispatch_commands([
        cyview
    ])
