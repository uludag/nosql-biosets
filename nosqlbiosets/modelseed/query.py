#!/usr/bin/env python
""" Queries with ModelSEEDDatabase data indexed with MongoDB """
import json
import re

from nosqlbiosets.dbutils import DBconnection

# MongoDB collection names or Elasticsearch index names
# for compounds and reactions
COMPOUNDSTYPE = "modelseed_compound"
REACTIONSTYPE = "modelseed_reaction"


def modelseeddb_parse_equation(equation, delimiter=' '):
    """ Copied from ModelSEEDDatabase Biochem_Helper.py:parseEquation()
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

    def __init__(self):
        self.index = "biosets"
        self.dbc = DBconnection("MongoDB", self.index)

    # Given ModelSEED compound id return its name
    def getcompoundname(self, dbc, mid, limit=0):
        if dbc.db == 'Elasticsearch':
            index, doctype = COMPOUNDSTYPE, "_doc"
            qc = {"match": {"_id": mid}}
            hits, n = self.esquery(dbc.es, index, qc, doctype)
        else:  # MongoDB
            doctype = COMPOUNDSTYPE
            qc = {"_id": mid}
            hits = list(dbc.mdbi[doctype].find(qc, limit=limit))
            n = len(hits)
        assert 1 == n
        c = hits[0]
        desc = c['name'] if 'name' in c else c['_source']['name']
        return desc

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
        doctype = COMPOUNDSTYPE
        hits = self.dbc.mdbi[doctype].find(qc, **kwargs)
        r = [c for c in hits]
        return r

    # Get network of metabolites ,
    # edges refer to the unique set of reactions connecting two metabolites
    def get_metabolite_network(self, qc):
        import networkx as nx
        graph = nx.DiGraph(name='metanetx', query=json.dumps(qc))
        reacts = self.dbc.mdbi[REACTIONSTYPE].find(qc)
        mre = re.compile(r'\((\d*\.*\d*(e-\d+)?)\) (cpd\d+)\[(\d+)\]')
        r = self.query_metabolites({}, projection=['name'])
        id2name = {i['_id']: i['name'] for i in r}
        for r in reacts:
            reactants, products, _ = \
                modelseeddb_parse_equation(r['equation'])
            if reactants is None:
                print(r['equation'])
                continue
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
                        er.add(r['_id'])
                    else:
                        er = {r['_id']}
                        graph.add_edge(u, v, reactions=er)
        return graph
