#!/usr/bin/env python
""" Queries with DrugBank data indexed with MongoDB """

import networkx as nx

from nosqlbiosets.dbutils import DBconnection
from nosqlbiosets.graphutils import *

DOCTYPE = 'drug'  # MongoDB collection name


class QueryDrugBank:
    index = "drugbank"
    db = "MongoDB"
    dbc = DBconnection(db, index)
    mdb = dbc.mdbi

    def query(self, qc, projection=None, limit=0):
        print("Querying with query clause '%s'" % (str(qc)))
        c = self.mdb[DOCTYPE].find(qc, projection=projection, limit=limit)
        return c

    def aggregate_query(self, agpl):
        r = self.mdb[DOCTYPE].aggregate(agpl)
        return r

    def distinctquery(self, key, qc=None, sort=None):
        r = self.dbc.mdbi[DOCTYPE].distinct(key, filter=qc, sort=sort)
        return r

    def get_connections(self, qc, connections):
        project = {"name": 1, connections + ".name": 1}
        r = self.query(qc, projection=project)
        interactions = list()
        for d in r:
            name = d['name']
            if connections in d:
                for t in d[connections]:
                    # TODO: return more information
                    interactions.append((name, t['name']))
        return interactions

    # Gets and saves networks from subsets of DrugBank records
    # filtered by query clause, qc. Graph file format is selected
    #  based on file extension used, as detailed in the readme.md file
    def get_connections_graph(self, qc, connections, outfile=None):
        interactions = self.get_connections(qc, connections)
        graph = nx.MultiDiGraph(list(interactions), name=connections)
        if outfile is not None:
            save_graph(graph, outfile)
        return graph

    def get_allnetworks(self, qc):
        connections = ["targets", "enzymes", "transporters", "carriers"]
        interactions = set()
        for connection in connections:
            interactions = interactions.union(
                set(self.get_connections(qc, connection)))
        graph = nx.MultiDiGraph(list(interactions), name="allnetworks")
        return graph


if __name__ == '__main__':
    import argparse
    import json
    parser = argparse.ArgumentParser(
        description='Save DrugBank interactions as NetworkX graph files')
    parser.add_argument('-qc', '--qc',
                        default='{"carriers.name": "Serum albumin"}',
                        help='MongoDB query clause to select subsets'
                             ' of DrugBank entries')
    parser.add_argument('-graphfile', '--graphfile',
                        help='File name for saving the output graph'
                             ' in GraphML, GML, Cytoscape.js or d3js formats,'
                             ' see readme.md for details')
    args = parser.parse_args()
    qry = QueryDrugBank()
    qc_ = json.loads(args.qc)
    g = qry.get_connections_graph(qc_, "targets", args.graphfile)
    print(nx.info(g))
