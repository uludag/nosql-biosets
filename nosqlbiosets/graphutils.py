""" Methods to return graphs in Cytoscape.js and D3js formats """


# Return NetworkX graphs in Cytoscape.js JSON format
# https://stackoverflow.com/questions/45342470/
# how-to-show-a-network-generated-by-networkx-in-cytoscape-js
def networkx2cytoscape_json(networkxgraph):
    cygraph = dict()
    cygraph["nodes"] = []
    cygraph["edges"] = []
    for node in networkxgraph.nodes():
        nx = dict()
        nx["data"] = {}
        nx["data"]["id"] = node
        nx["data"]["label"] = node
        cygraph["nodes"].append(nx.copy())
    for edge in networkxgraph.edges():
        nx = dict()
        nx["data"] = {}
        nx["data"]["id"] = edge[0] + edge[1]
        nx["data"]["source"] = edge[0]
        nx["data"]["target"] = edge[1]
        cygraph["edges"].append(nx)
    return cygraph


def networkx2d3_json(networkxgraph):
    d3 = dict()
    d3["nodes"] = []
    d3["links"] = []
    for node in networkxgraph.nodes():
        nx = dict()
        nx["id"] = node
        nx["label"] = node
        d3["nodes"].append(nx.copy())
    for edge in networkxgraph.edges():
        nx = dict()
        nx["source"] = edge[0]
        nx["target"] = edge[1]
        d3["links"].append(nx)
    return d3
