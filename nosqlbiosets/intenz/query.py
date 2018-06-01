#!/usr/bin/env python
""" Query IntEnz data indexed with MongoDB or Neo4j """
# Server connection details are read from  conf/dbservers.json file

import argparse
import json

import networkx as nx

from nosqlbiosets.dbutils import DBconnection
from nosqlbiosets.graphutils import save_graph

COLLECTION = "intenz"


class QueryIntEnz:

    def __init__(self, db="MongoDB", index="biosets", doctype="intenz"):
        self.doctype = doctype
        self.dbc = DBconnection(db, index)

    def getreactantnames(self, filterc=None):
        assert self.dbc.db == 'MongoDB'
        key = "reactions.reactantList.reactant.title"
        r = self.dbc.mdbi[self.doctype].distinct(key, filter=filterc)
        return r

    def getproductnames(self, filterc=None):
        assert self.dbc.db == 'MongoDB'
        key = "reactions.productList.product.title"
        r = self.dbc.mdbi[self.doctype].distinct(key, filter=filterc)
        return r

    # Find enzyme names for given query
    def getenzymenames(self, qc):
        if self.dbc.db == 'MongoDB':
            pr = ["_id", "accepted_name.#text"]
            hits = self.dbc.mdbi[self.doctype].find(qc, projection=pr)
            hits = [c for c in hits]
            # TODO: accepted_name is list
            r = [(c['_id'], c["accepted_name"]["#text"])
                 for c in hits if "accepted_name" in c and
                 not isinstance(c["accepted_name"], list)]
            return r

    # Find enzymes where given chemical is a reactant
    def getenzymeswithreactant(self, reactant):
        assert self.dbc.db == 'MongoDB'
        qc = {
            "reactions.reactantList.reactant.title": reactant}
        r = self.getenzymenames(qc)
        return r

    # Find enzymes where given chemical is a reactant
    def getenzymeswithreactant_chebiid(self, chebiid):
        assert self.dbc.db == 'MongoDB'
        qc = {
            "reactions.reactantList."
            "reactant.molecule.identifier.value": "CHEBI:%d" % chebiid}
        r = self.getenzymenames(qc)
        return r

    # Find enzymes where given chemical is a product
    def getenzymeswithproduct_chebiid(self, chebiid):
        assert self.dbc.db == 'MongoDB'
        qc = {
            "reactions.productList."
            "product.molecule.identifier.value": "CHEBI:%d" % chebiid}
        r = self.getenzymenames(qc)
        return r

    # Get names of the enzymes with given ids
    def getenzymeswithids(self, eids):
        if self.dbc.db == 'MongoDB':
            qc = {"_id": {'$in': eids}}
            r = self.getenzymenames(qc)
            return r

    def getenzymebyid(self, eid=None):
        if self.dbc.db == 'MongoDB':
            r = self.dbc.mdbi[self.doctype].find_one(filter=eid)
            return r

    # Find all enzymes where given chemical is a product
    def query_products(self, rnames):
        if self.dbc.db == 'MongoDB':
            qc = {
                "reactions.productList.product.title": {
                    '$in': rnames}}
            r = ["_id", "accepted_name.#text"]
            hits = self.dbc.mdbi[self.doctype].find(qc, projection=r, limit=100)
            r = [c for c in hits]
            return r

    def enzyme_name2id(self, enames):
        if self.dbc.db == 'MongoDB':
            qc = {"accepted_name.#text": {'$in': enames}}
            hits = self.dbc.mdbi[self.doctype].find(qc, limit=10)
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
                        "reactions.reactantList.reactant.title": {
                            '$in': [reactant]},
                        "reactions.productList.product.title": {
                            '$in': [product]}},
                    {
                        "reactions.reactantList.reactant.title": {
                            '$in': [product]},
                        "reactions.productList.product.title": {
                            '$in': [reactant]}}
                ]}
            hits = self.dbc.mdbi[self.doctype].find(qc)
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
                "reactions.reactantList.reactant.title": source}},
            {"$unwind": "$reactions"},
            {"$match": {
                "reactions.productList": {"$exists": True}}},
            {"$project": {
                "reactions.productList.product.title": 1,
                "reactions.reactantList.reactant.title": 1,
                "accepted_name.#text": 1
            }},
            {"$lookup": {
                "from": "intenz",
                "localField":
                    "reactions.productList.product.title",
                "foreignField":
                    "reactions.reactantList.reactant.title",
                "as": "enzyme2"
            }},
            {"$unwind": "$enzyme2"},
            {"$match": {
                "enzyme2.reactions.productList.product":
                    {'$elemMatch': {"title": target}}}},
            {"$project": {
                "accepted_name.#text": 1,
                "reactions.productList.product.title": 1,
                "reactions.reactantList.reactant.title": 1,
                "enzyme2.depth": 1,
                "enzyme2._id": 1,
                "enzyme2.accepted_name.#text": 1,
                "enzyme2.reactions.productList.product.title": 1,
                "enzyme2.reactions.reactantList.reactant.title": 1
            }}
        ]
        hits = self.dbc.mdbi[self.doctype].aggregate(agpl)
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
                "reactions.reactantList.reactant.title": source}},
            {"$unwind": "$reactions"},
            {"$match": {
                "reactions.reactantList.reactant.title": {"$exists": True}}},
            {"$match": {
                "reactions.productList.product.title": {"$exists": True}}},
            {"$project": {
                "reactions.productList.product.title": 1,
                "reactions.reactantList.reactant.title": 1,
                "accepted_name.#text": 1
            }},
            {"$graphLookup": {
                "from": "intenz",
                "startWith": "$reactions.productList.product.title",
                "connectToField":
                    "reactions.reactantList.reactant.title",
                "connectFromField":
                    "reactions.productList.product.title",
                "as": "enzymes",
                "maxDepth": depth,
                "depthField": "depth",
                "restrictSearchWithMatch": graphfilter
            }},
            {"$project": {
                "accepted_name.#text": 1,
                "enzymes.depth": 1,
                "reactions.productList.product.title": 1,
                "reactions.reactantList.reactant.title": 1,
                "enzymes._id": 1,
                "enzymes.accepted_name.#text": 1,
                "enzymes.reactions.productList.product.title": 1,
                "enzymes.reactions.reactantList.reactant.title": 1
            }},
            {"$unwind": "$enzymes"},
            {"$unwind": "$enzymes.reactions"},
            {"$match": {
                "enzymes.reactions.reactantList.reactant.title": {
                    "$exists": True}}},
            {"$match": {
                "enzymes.reactions.productList.product.title": {
                    "$exists": True}}},
            {"$match": {
                "$or": [{"depth": {"$lt": depth}},
                        {"enzymes.reactions.productList.product": {
                            '$elemMatch': {"title": target}}}]}},
        ]
        hits = self.dbc.mdbi[self.doctype].aggregate(agpl)
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
            hits = self.dbc.mdbi[self.doctype].aggregate(agpl)
            r = [i for i in hits]
            return r

    # connections are either from reactants to reactions
    # or from reactions to products
    def get_connections(self, filterc, limit=40000):
        assert self.dbc.db == 'MongoDB'
        agpl = [
            {"$match": filterc},
            {"$project": {
                "reactions": 1
            }},
            {"$unwind": "$reactions"},
            {"$unwind": "$reactions.reactantList.reactant"},
            {"$unwind": "$reactions.productList.product"},
            {"$group": {
                "_id": {
                    "enzyme": "$_id",
                    "reactant":
                        "$reactions.reactantList.reactant.title",
                    "product": "$reactions.productList.product.title"
                },
            }},
            {"$limit": limit}
        ]
        r = self.dbc.mdbi[self.doctype].aggregate(agpl)
        connections = list()
        for i in r:
            connections.append(i['_id'])
        return connections

    def get_connections_graph(self, qc, limit):
        connections = self.get_connections(qc, limit)
        graph = nx.MultiDiGraph(name="IntEnz query %s" % json.dumps(qc))
        for c in connections:
            graph.add_node(c['reactant'], type='reactant', color='green')
            graph.add_node(c['product'], type='product', color='orange')
            graph.add_node(c['enzyme'], type='enzyme', color='brown')
            graph.add_edge(c['reactant'], c['enzyme'])
            graph.add_edge(c['enzyme'], c['product'])
        return graph


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Save IntEnz reaction connections as graph files')
    parser.add_argument('qc',
                        help='MongoDB query clause to select subsets'
                             ' of IntEnz entries,'
                             ' e.g.: \'{"reactions.label.value": '
                             '"Chemically balanced"}\'')
    parser.add_argument('outfile',
                        help='File name for saving the output graph. '
                             'Format is selected based on the file extension'
                             ' of the given output file;'
                             ' .xml for GraphML, .gml for GML,'
                             ' .js for Cytoscape.js,'
                             ' or .d3js.json for d3js format')
    parser.add_argument('--limit',
                        default=3000, type=int,
                        help='Maximum number of enzyme-metabolite connections')
    args = parser.parse_args()
    qry = QueryIntEnz()
    qc_ = json.loads(args.qc)
    cgraph = qry.get_connections_graph(qc_, args.limit/2)
    print(nx.info(cgraph))
    save_graph(cgraph, args.outfile)
