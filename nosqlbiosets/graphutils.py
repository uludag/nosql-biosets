""" Methods to return NetworkX graphs in Cytoscape.js or D3js formats """
import json

import networkx


# Return input NetworkX graph in Cytoscape.js JSON format
def networkx2cytoscape_json(networkxgraph):
    nodes = []
    edges = []
    for node in networkxgraph.nodes():
        nodes.append({'data': {'id': node, 'label': node}})
    for edge in networkxgraph.edges():
        data = {
            "id": edge[0] + edge[1],
            "source": edge[0],
            "target": edge[1]
        }
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
        networkx.write_graphml(graph, outfile)
    elif outfile.endswith(".d3js.json"):
        cygraph = networkx2d3_json(graph)
        json.dump(cygraph, open(outfile, "w"), indent=4)
    elif outfile.endswith(".json"):
        cygraph = networkx2cytoscape_json(graph)
        json.dump(cygraph, open(outfile, "w"), indent=4)
    else:  # Assume GML format
        networkx.write_gml(graph, outfile)
    print('Network file saved: ' + outfile)
