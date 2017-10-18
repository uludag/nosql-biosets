#!/usr/bin/env python
""" Queries with IntEnz data indexed with MongoDB """

from ..dbutils import DBconnection


class QueryIntEnz:

    def __init__(self):
        index = "biosets"
        self.doctype = "intenz"
        self.dbc = DBconnection("MongoDB", index)

    def getreactantnames(self, filterc=None):
        if self.dbc.db == 'MongoDB':
            key = "reactions.reaction.reactantList.reactant.title"
            r = self.dbc.mdbi[self.doctype].distinct(key, filter=filterc)
            print("#reactants = %d" % len(r))
            return r

    def getproductnames(self, filterc=None):
        if self.dbc.db == 'MongoDB':
            key = "reactions.reaction.productList.product.title"
            r = self.dbc.mdbi[self.doctype].distinct(key, filter=filterc)
            print("#products = %d" % len(r))
            return r

    # Find enzyme names for given query
    def getenzymenames(self, qc):
        if self.dbc.db == 'MongoDB':
            r = ["_id", "accepted_name.#text"]
            hits = self.dbc.mdbi[self.doctype].find(qc, projection=r)
            hits = [c for c in hits]
            # TODO: accepted_name is list
            r = [(c[r[0]], c["accepted_name"]["#text"])
                 for c in hits if "accepted_name" in c and
                 not isinstance(c["accepted_name"], list)]
            return r

    # Find enzymes where one of the given chemicals is a reactant
    def getenzymeswithreactants(self, reactants):
        if self.dbc.db == 'MongoDB':
            qc = {
                "reactions.reaction.reactantList.reactant.title": {
                    '$in': reactants}}
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
            print("#enzyme name = %s" % r["accepted_name"]["#text"])
            return r

    # Find all enzymes where given chemical is a product
    def query_products(self, rnames):
        if self.dbc.db == 'MongoDB':
            qc = {
                "reactions.reaction.productList.product.title": {
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
                "reactions.reaction.convention": "rhea:direction.BI",
                "$or": [
                    {
                        "reactions.reaction.reactantList.reactant.title": {
                            '$in': [reactant]},
                        "reactions.reaction.productList.product.title": {
                            '$in': [product]}},
                    {
                        "reactions.reaction.reactantList.reactant.title": {
                            '$in': [product]},
                        "reactions.reaction.productList.product.title": {
                            '$in': [reactant]}}
                ]}
            hits = self.dbc.mdbi[self.doctype].find(qc, limit=10)
            hits = [c for c in hits]
            eids = [c['accepted_name']['#text'] for c in hits]
            return eids

    # For metabolite paths with 2 enzymes
    # Find 2 enzymes that the first enzyme catalyses a reaction
    # with given reactant(source)
    # and the second enzyme for the given product(target)
    def lookup_connected_metabolites(self, source, target):
        agpl = [
            {"$match": {
                "reactions.reaction.reactantList.reactant":
                    {'$elemMatch': {"title": source}}}},
            {"$project": {
                "reactions.reaction.map": 0,
                "links": 0,
                "references": 0
            }},
            {"$unwind": {"path": "$reactions.reaction",
                         "includeArrayIndex": "rindex"}},
            {"$match": {
                "reactions.reaction.productList": {"$exists": True}}},
            {"$lookup": {
                "from": "intenz",
                "localField":
                    "reactions.reaction.productList.product.title",
                "foreignField":
                    "reactions.reaction.reactantList.reactant.title",
                "as": "enzyme2"
            }},
            {"$unwind": "$enzyme2"},
            {"$match": {
                "enzyme2.reactions.reaction.productList.product":
                    {'$elemMatch': {"title": target}}}},
            {"$group": {"_id": "$accepted_name.#text",
                        "enzyme2": {"$push": "$enzyme2.accepted_name.#text"}}}
        ]
        hits = self.dbc.mdbi[self.doctype].aggregate(agpl)
        r = [i for i in hits]
        return r

    # For paths with 2 or more enzymes. Not fully implemented yet
    def graphlookup_connected_metabolites(self, source, target, depth=0,
                                          graphfilter=None):
        if graphfilter is None:
            graphfilter = {}
        agpl = [
            {"$match": {
                "reactions.reaction.reactantList.reactant":
                    {'$elemMatch': {"title": source}}}},
            {"$project": {
                "reactions.reaction.map": 0,
                "links": 0, "references": 0, "comments": 0
            }},
            {"$unwind": {"path": "$reactions.reaction",
                         "includeArrayIndex": "rindex"}},
            {"$match": {
                "reactions.reaction.productList": {"$exists": True}}},
            {"$graphLookup": {
                "from": "intenz",
                "startWith": source,
                "connectToField":
                    "reactions.reaction.reactantList.reactant.title",
                "connectFromField":
                    "reactions.reaction.productList.product.title",
                "as": "enzymes",
                "maxDepth": depth,
                "depthField": "depth",
                "restrictSearchWithMatch": graphfilter
            }},
            {"$unwind": "$enzymes"},
            {"$match": {"$or": [ {"depth": {"$lt": depth}},
                {"enzymes.reactions.reaction.productList.product":
                    {'$elemMatch': {"title": target}}}]}},
            {"$group": {"_id": "$accepted_name.#text",
                        "enzymes": {"$push": "$enzymes.accepted_name.#text"}}}
        ]
        hits = self.dbc.mdbi[self.doctype].aggregate(agpl)
        r = [i for i in hits]
        return r
