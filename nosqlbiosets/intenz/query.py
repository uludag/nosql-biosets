#!/usr/bin/env python
""" Simple queries with IntEnz data indexed with MongoDB """

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
            hits = self.dbc.mdbi[self.doctype].find(qc, projection=r, limit=100)
            r = [(c[r[0]], c["accepted_name"]["#text"]) for c in hits]
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
            # update query in case of
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
            # print(json.dumps(hits, indent=4))
            eids = [c['accepted_name']['#text'] for c in hits]
            return eids
