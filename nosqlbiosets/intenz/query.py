#!/usr/bin/env python
""" Query IntEnz data indexed with MongoDB or Neo4j """
# Server connection details are read from file conf/dbservers.json

import json

import argh
import networkx as nx

from nosqlbiosets.graphutils import save_graph
from nosqlbiosets.qryutils import parseinputquery, Query

COLLECTION = "intenz"


class QueryIntEnz(Query):

    def __init__(self, dbtype="MongoDB", index="biosets",
                 mdbcollection=COLLECTION, **kwargs):
        super(QueryIntEnz, self).__init__(dbtype, index,
                                          mdbcollection, **kwargs)

    def getreactantnames(self, filterc=None, **kwargs):
        assert self.dbc.db == 'MongoDB'
        key = "reactions.reactants.title"
        r = self.dbc.mdbi[self.mdbcollection].distinct(key, filter=filterc, **kwargs)
        return r

    def getproductnames(self, filterc=None):
        assert self.dbc.db == 'MongoDB'
        key = "reactions.products.title"
        r = self.dbc.mdbi[self.mdbcollection].distinct(key, filter=filterc)
        return r

    def getcofactors(self, filterc=None):
        """A cofactor is any non-protein substance required
         for a protein to be catalytically active
         http://www.uniprot.org/help/cofactor
         https://en.wikipedia.org/wiki/Cofactor_(biochemistry)
         """
        assert self.dbc.db == 'MongoDB'
        key = "cofactors"
        r = self.dbc.mdbi[self.mdbcollection].distinct(key, filter=filterc)
        return r

    # Find enzyme names for given query
    def getenzymenames(self, qc):
        if self.dbc.db == 'MongoDB':
            pr = ["_id", "accepted_name.#text", "history"]
            hits = self.dbc.mdbi[self.mdbcollection].find(qc, projection=pr)
            r = dict()
            for c in hits:
                if "accepted_name" in c:
                    if isinstance(c["accepted_name"], list):
                        r[c['_id']] = c["accepted_name"][0]["#text"]
                    else:
                        r[c['_id']] = c["accepted_name"]["#text"]
                else:
                    r[c['_id']] = c["history"]
            return r

    # Find enzymes where given chemical is a reactant
    def getenzymeswithreactant(self, reactant):
        assert self.dbc.db == 'MongoDB'
        qc = {
            "reactions.reactants.title": reactant}
        r = self.getenzymenames(qc)
        return r

    # Find enzymes where given chemical is a reactant
    def getenzymeswithreactant_chebiid(self, chebiid):
        assert self.dbc.db == 'MongoDB'
        qc = {
            "reactions.reactants."
            "molecule.identifier.value": "CHEBI:%d" % chebiid}
        r = self.getenzymenames(qc)
        return r

    # Find enzymes where given chemical is a product
    def getenzymeswithproduct(self, product):
        assert self.dbc.db == 'MongoDB'
        qc = {
            "reactions.products.title": product}
        r = self.getenzymenames(qc)
        return r

    # Find enzymes where given chemical is a product
    def getenzymeswithproduct_chebiid(self, chebiid):
        assert self.dbc.db == 'MongoDB'
        qc = {
            "reactions.products."
            "molecule.identifier.value": "CHEBI:%d" % chebiid}
        r = self.getenzymenames(qc)
        return r

    # Get names of the enzymes with given ids
    def getenzymeswithids(self, eids):
        if self.dbc.db == 'MongoDB':
            qc = {
                "$or": [
                    {"_id": {'$in': eids}}
                ]
            }
            for eid in eids:
                if eid[-1] == '-':
                    qc["$or"].append(
                        {"_id": {"$regex": "^%s" % eid[:-1]}}
                    )
            r = self.getenzymenames(qc)
            return r

    def getenzymebyid(self, eid=None):
        if self.dbc.db == 'MongoDB':
            r = self.dbc.mdbi[self.mdbcollection].find_one(filter=eid)
            return r

    # Find all enzymes where given chemical is a product
    def query_products(self, rnames):
        if self.dbc.db == 'MongoDB':
            qc = {
                "reactions.products.title": {
                    '$in': rnames}}
            r = ["_id", "accepted_name.#text"]
            hits = self.dbc.mdbi[self.mdbcollection].find(qc, projection=r, limit=100)
            r = [c for c in hits]
            return r

    def enzyme_name2id(self, enames):
        if self.dbc.db == 'MongoDB':
            qc = {"accepted_name.#text": {'$in': enames}}
            hits = self.dbc.mdbi[self.mdbcollection].find(qc, limit=10)
            eids = [c['_id'] for c in hits]
            return eids

    # Find all enzymes that catalyses a reaction with given reactant and product
    def query_reactantandproduct(self, reactant, product):
        if self.dbc.db == 'MongoDB':
            # TODO: reaction directions
            qc = {
                "reactions.convention": "rhea:direction.UN",
                "$or": [
                    {
                        "reactions.reactants.title": {
                            '$in': [reactant]},
                        "reactions.products.title": {
                            '$in': [product]}},
                    {
                        "reactions.reactants.title": {
                            '$in': [product]},
                        "reactions.products.title": {
                            '$in': [reactant]}}
                ]}
            hits = self.dbc.mdbi[self.mdbcollection].find(qc)
            hits = [c for c in hits]
            eids = [c['accepted_name']['#text'] for c in hits]
            return eids

    # For paths with 2 enzymes
    # Find 2 enzymes that the first enzyme catalyses a reaction
    # with given reactant(source)
    # AND the second enzyme for the given product(target)
    # AND both enzymes are connected with e1.product == e2.reactant
    def lookup_connected_metabolites(self, source, target):
        agpl = [
            {"$match": {
                "reactions.reactants.title": source}},
            {"$unwind": "$reactions"},
            {"$match": {
                "reactions.products": {"$exists": True}}},
            {"$project": {
                "reactions.products.title": 1,
                "reactions.reactants.title": 1,
                "accepted_name.#text": 1
            }},
            {"$lookup": {
                "from": "intenz",
                "localField":
                    "reactions.products.title",
                "foreignField":
                    "reactions.reactants.title",
                "as": "enzyme2"
            }},
            {"$unwind": "$enzyme2"},
            {"$match": {
                "enzyme2.reactions.products":
                    {'$elemMatch': {"title": target}}}},
            {"$project": {
                "accepted_name.#text": 1,
                "reactions.products.title": 1,
                "reactions.reactants.title": 1,
                "enzyme2.depth": 1,
                "enzyme2._id": 1,
                "enzyme2.accepted_name.#text": 1,
                "enzyme2.reactions.products.title": 1,
                "enzyme2.reactions.reactants.title": 1
            }}
        ]
        hits = self.dbc.mdbi[self.mdbcollection].aggregate(agpl)
        r = [i for i in hits]
        return r

    # For paths with 2 or more enzymes
    # Easily hits MongoDB maximum memory limit
    def graphlookup_connected_metabolites(self, source, target, depth=0,
                                          graphfilter=None):
        if graphfilter is None:
            graphfilter = {}
        agpl = [
            {"$match": {
                "reactions.reactants.title": source}},
            {"$unwind": "$reactions"},
            {"$match": {
                "reactions.reactants.title": {"$exists": True}}},
            {"$match": {
                "reactions.products.title": {"$exists": True}}},
            {"$project": {
                "reactions.products.title": 1,
                "reactions.reactants.title": 1,
                "accepted_name.#text": 1
            }},
            {"$graphLookup": {
                "from": "intenz",
                "startWith": "$reactions.products.title",
                "connectToField":
                    "reactions.reactants.title",
                "connectFromField":
                    "reactions.products.title",
                "as": "enzymes",
                "maxDepth": depth,
                "depthField": "depth",
                "restrictSearchWithMatch": graphfilter
            }},
            {"$project": {
                "accepted_name.#text": 1,
                "enzymes.depth": 1,
                "reactions.products.title": 1,
                "reactions.reactants.title": 1,
                "enzymes._id": 1,
                "enzymes.accepted_name.#text": 1,
                "enzymes.reactions.products.title": 1,
                "enzymes.reactions.reactants.title": 1
            }},
            {"$unwind": "$enzymes"},
            {"$unwind": "$enzymes.reactions"},
            {"$match": {
                "enzymes.reactions.reactants.title": {
                    "$exists": True}}},
            {"$match": {
                "enzymes.reactions.products.title": {
                    "$exists": True}}},
            {"$match": {
                "$or": [{"depth": {"$lt": depth}},
                        {"enzymes.reactions.products": {
                            '$elemMatch': {"title": target}}}]}},
        ]
        hits = self.dbc.mdbi[self.mdbcollection].aggregate(agpl)
        r = [i for i in hits]
        return r

    def neo4j_shortestpathsearch_connected_metabolites(self, source, target,
                                                       k=5):
        q = 'MATCH (source_:Substrate{id:{source}}),' \
            ' (target_:Product{id:{target}}),' \
            '  path=allShortestPaths((source_)-[*..' + str(k)\
            + ']->(target_)) RETURN path'
        r = self.dbc.neo4jc.run(q, source=source, target=target)
        return r

    def getreactions(self, filterc=None):
        if self.dbc.db == 'Neo4j':
            q = 'MATCH (r:Reaction) RETURN r'
            r = self.dbc.neo4jc.run(q)
            return r
        if self.dbc.db == 'MongoDB':
            agpl = [
                {"$match": filterc},
                {"$project": {
                    "reactions": 1
                }},
                {"$unwind": {"path": "$reactions"}},
                {"$group": {"_id": "$reactions.id",
                            "reaction": {"$addToSet": "$reactions"}}}
            ]
            hits = self.dbc.mdbi[self.mdbcollection].aggregate(agpl)
            r = [i for i in hits]
            return r

    # Connections are from reactants to products, reactions are edges
    def get_connections(self, filterc, limit=40000):
        assert self.dbc.db == 'MongoDB'
        agpl = [
            {"$match": filterc},
            {"$project": {
                "reactions": 1
            }},
            {"$unwind": "$reactions"},
            {"$unwind": "$reactions.reactants"},
            {"$unwind": "$reactions.products"},
            {"$group": {
                "_id": {
                    "reactant":
                        "$reactions.reactants.title",
                    "product": "$reactions.products.title"
                },
                "enzymes": {"$addToSet": "$_id"},
                "count": {"$sum": 1}
            }},
            {"$limit": limit}
        ]
        r = self.dbc.mdbi[self.mdbcollection].aggregate(agpl)
        return r

    def get_connections_graph(self, qc, limit=4000):
        connections = self.get_connections(qc, limit)
        graph = nx.MultiDiGraph(name="IntEnz query %s" % json.dumps(qc))
        # Set enzymes as edge attributes
        for c_ in connections:
            c = c_['_id']
            # TODO: edged graph mean no need to say reactant product
            graph.add_node(c['reactant'], type='reactant', viz_color='brown')
            graph.add_node(c['product'], type='product', viz_color='orange')
            graph.add_edge(c['reactant'], c['product'], ec=c_['enzymes'])
        return graph


def savegraph(query, outfile, limit=4000):
    """Save IntEnz reaction connections as graph files

    param: qc: MongoDB query clause to select subsets of IntEnz entries,
               e.g.: \'{"reactions.label.value": ' '"Chemically balanced"}\'
    param: outfile: File name for saving the output graph
                    Format is selected based on the file extension
                    of the given output file;
                             .xml for GraphML, .gml for GML,
                             .json for Cytoscape.js
    param: --limit: Maximum number of enzyme-metabolite connections
    """
    qry = QueryIntEnz()
    qc = parseinputquery(query)
    cgraph = qry.get_connections_graph(qc, int(limit/2))
    print(nx.info(cgraph))
    save_graph(cgraph, outfile)


def cyview(query, name='', limit=4000):
    """ See IntEnz enzyme graphs with Cytoscape runing on your local machine

     :param query: Query to select IntEnz entries
     :param name: Name of the graph on Cytoscape, query is used as default value
     :param limit
     """
    from py2cytoscape.data.cyrest_client import CyRestClient
    from py2cytoscape.data.style import Style
    qc = parseinputquery(query)
    qry = QueryIntEnz()
    mn = qry.get_connections_graph(qc, limit)
    crcl = CyRestClient()
    mn.name = json.dumps(qc) if name == '' else name
    cyn = crcl.network.create_from_networkx(mn)
    crcl.layout.apply('kamada-kawai', network=cyn)
    crcl.style.apply(Style('default'), network=cyn)


if __name__ == '__main__':
    argh.dispatch_commands([
        savegraph, cyview
    ])
