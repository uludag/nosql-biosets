""" Methods to return NetworkX graphs in Cytoscape.js or D3js formats """
import json

import networkx as nx


# Return input NetworkX graph in Cytoscape.js JSON format
# TODO: replace with py2cytoscape calls, https://py2cytoscape.readthedocs.io/
def networkx2cytoscape_json(networkxgraph):
    nodes = []
    edges = []

    for node in networkxgraph.nodes():
        node_ = {'data': {
            'id': node,
            'name': networkxgraph.nodes[node]['name']
            if 'name' in networkxgraph.nodes[node] else node,
            'label': networkxgraph.nodes[node]['label']
            if 'label' in networkxgraph.nodes[node] else node}
        }
        if 'viz_color' in networkxgraph.nodes[node]:
            node_['data']['viz_color'] = networkxgraph.nodes[node]['viz_color']
        nodes.append(node_)
    for edge in networkxgraph.edges():
        data = {
            "id": edge[0] + edge[1],
            "source": edge[0],
            "target": edge[1]
        }
        ed = networkxgraph.get_edge_data(edge[0], edge[1])
        if 0 in ed:  # MultiDiGraph, select the first edge only
            ed = ed[0]

        for key in ed:
            val = ed[key]
            if isinstance(val, set):
                val = list(val)
            data[key] = val

        edges.append({'data': data})
    cygraph = {
        'data': {
            'name': networkxgraph.name
        },
        'elements': {
            'nodes': nodes, 'edges': edges
        },
        'style': [
            {
                'selector': 'node',
                'style': {
                    'background-color': '#666',
                    'label': 'data(id)'
                }},
            {
                'selector': 'edge',
                'style': {
                    'width': 3,
                    'line-color': '#ccc',
                    'target-arrow-color': '#ccc',
                    'target-arrow-shape': 'triangle'
                }}
        ],
        'layout': {
            'name': 'cose',
            'directed': True,
            'nodeDimensionsIncludeLabels': True
        }
    }
    return cygraph


def networkx2d3_json(networkxgraph):
    d3 = dict()
    d3["nodes"] = []
    d3["links"] = []
    for node in networkxgraph.nodes():
        d3["nodes"].append({"id": node, "label": node})
    for edge in networkxgraph.edges():
        d3["links"].append({"source": edge[0], "target": edge[1]})
    return d3


# Save NetworkX graph in one of four formats.
# Format is selected based on the file extension of the given output file.
# If the file name ends with .xml suffix [GraphML](
#    https://en.wikipedia.org/wiki/GraphML) format is selected,
# If the file name ends with .d3.json extension graph is saved in
# a form easier to read with [D3js](d3js.org),
# If the file name ends with .json extension graph is saved in
# [Cytoscape.js](js.cytoscape.org) graph format,
# Otherwise it is saved in GML format
def save_graph(graph, outfile):
    if outfile.endswith(".xml"):
        nx.write_graphml(graph, outfile)
    elif outfile.endswith(".d3js.json"):
        cygraph = networkx2d3_json(graph)
        json.dump(cygraph, open(outfile, "w"), indent=4)
    elif outfile.endswith(".json"):
        cygraph = networkx2cytoscape_json(graph)
        json.dump(cygraph, open(outfile, "w"), indent=4)
    else:  # Assume GML format
        nx.write_gml(graph, outfile)
    print('Network file saved: ' + outfile)


def set_degree_as_weight(g):
    """Set degree of connected nodes as weight.
       For metabolite graphs it is often desirable to see the routes with
       less connected metabolites
    """
    d = nx.degree_centrality(g)
    for u, v in g.edges():
        g[u][v]['weight'] = d[v]


def shortest_paths(dg, source, target, k=None, cutoff=10,
                   weight=None, degreeasweight=False):
    # NetworkX shortest_simple_paths function returns paths generator
    # for the input graph from source to target starting from shortest ones
    if degreeasweight:
        dg = dg.copy()
        set_degree_as_weight(dg)
        weight = 'weight'
    gr = nx.shortest_simple_paths(dg, source=source, target=target,
                                  weight=weight)
    if k is None:
        return gr
    else:
        i, r = 0, []
        for path in gr:
            if len(path) > cutoff:
                break
            r.append(path)
            i += 1
            if i >= k:
                break
        return r


def neighbors_graph(ingraph, source, beamwidth=4, maxnodes=10):
    """ Neighbors of source node in ingraph """
    assert ingraph.is_directed(), "not implemented for undirected graphs"
    centrality = nx.eigenvector_centrality_numpy(ingraph)  # max_iter=10 tol=0.1
    outgraph = nx.MultiDiGraph()
    for u, v in nx.bfs_beam_edges(ingraph, source, centrality.get, beamwidth):
        if isinstance(ingraph, nx.MultiDiGraph):
            outgraph.add_edge(u, v, key=0, **(ingraph.get_edge_data(u, v)[0]))
        else:
            outgraph.add_edge(u, v, **(ingraph.get_edge_data(u, v)))
        if outgraph.number_of_nodes() >= maxnodes:
            break
    return outgraph


# Copied from Cameo project, http://cameo.bio/
# Original code was using NetworkX-1 API
def remove_highly_connected_nodes(network, max_degree=10):
    to_remove = [node for node, degree in network.degree()
                 if degree > max_degree]
    network.remove_nodes_from(to_remove)


def remove_least_connected_nodes(network, min_degree=1):
    to_remove = [node for node, degree in network.degree()
                 if degree <= min_degree]
    network.remove_nodes_from(to_remove)


def remove_small_subgraphs(ingraph, min_nodes=5):
    """ Remove subgraphs with less than given number of nodes """
    ingraph_ = ingraph.to_undirected()
    to_remove = set()
    for node, degree in ingraph.degree():
        if degree < min_nodes:
            r = set()
            for u, v in nx.dfs_edges(ingraph_, node, min_nodes):
                r.add(u)
                r.add(v)
                if len(r) >= min_nodes:
                    break
            if len(r) < min_nodes:
                to_remove = to_remove.union(r)
    ingraph.remove_nodes_from(to_remove)
