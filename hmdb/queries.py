#!/usr/bin/env python
""" Queries with HMDB and DrugBank data indexed with MongoDB """

from hmdb.drugbank import DOCTYPE  # Default MongoDB collection name
from hmdb.index import DOCTYPE_METABOLITE, DOCTYPE_PROTEIN
from nosqlbiosets.dbutils import DBconnection
from nosqlbiosets.graphutils import *
from nosqlbiosets.uniprot.query import QueryUniProt

index = "biosets"
db = "MongoDB"


class QueryDrugBank:
    dbc = DBconnection(db, index)
    mdb = dbc.mdbi

    def query(self, qc, projection=None, limit=0):
        print("Querying with query clause '%s'" % (str(qc)))
        c = self.mdb[DOCTYPE].find(qc, projection=projection, limit=limit)
        return c

    def aggregate_query(self, agpl, **kwargs):
        r = self.mdb[DOCTYPE].aggregate(agpl, **kwargs)
        return r

    def distinctquery(self, key, qc=None, sort=None):
        r = self.dbc.mdbi[DOCTYPE].distinct(key, filter=qc, sort=sort)
        return r

    def autocomplete_drugnames(self, qterm, **kwargs):
        """
        Given partial drug names return possible names
        :param qterm: partial drug name
        :return: list of possible names
        """
        qc = {"$or": [
            {"name": {
                "$regex": "^%s" % qterm, "$options": "i"}},
            {"abbreviation": {
                "$regex": "^%s" % qterm, "$options": "i"}},
            {"products.name": {
                "$regex": "^%s" % qterm, "$options": "i"}}
        ]}
        cr = self.mdb[DOCTYPE].find(qc, projection=['name'], **kwargs)
        return cr

    # Target genes and interacted drugs
    def get_target_genes_interacted_drugs(self, qc, limit=1600):
        aggqc = [
            {"$match": qc},
            {'$unwind': "$drug-interactions"},
            {'$unwind': "$targets"},
            {'$unwind': "$targets.polypeptide"},
            {'$group': {
                '_id': {
                    "drug": "$name",
                    'targetid': '$targets.polypeptide.gene-name',
                    'target': '$targets.polypeptide.name',
                    "idrug": "$drug-interactions.name"
                }}},
            {"$limit": limit}
        ]
        cr = self.mdb[DOCTYPE].aggregate(aggqc, allowDiskUse=True)
        r = []
        for i in cr:
            assert 'targetid' in i['_id']
            gid = i['_id']
            row = (gid['drug'], gid['targetid'], gid['target'],
                   gid['idrug'])
            r.append(row)
        return r

    def kegg_target_id_to_drugbank_entity_id(self, keggtid, etype='targets'):
        """
        Get drugbank target ids for given KEGG target ids
        The two databases are connected by first making a UniProt query
        :param keggtid: KEGG target id
        :param etype: Drugbank entity type, 'targets' or 'enzymes'
        :return: Drugbank target id
        """
        qryuniprot = QueryUniProt("MongoDB", "biosets", "uniprot")
        qc = {"dbReference.id": keggtid}
        key = 'name'
        uniprotid = qryuniprot.dbc.mdbi['uniprot'].distinct(key, filter=qc)
        assert 1 == len(uniprotid)
        qc = {
            etype+".polypeptide.external-identifiers.identifier": uniprotid[0]}
        aggq = [
            {"$match": qc},
            {'$unwind': "$"+etype},
            {'$unwind': "$"+etype+".polypeptide"},
            {"$match": qc},
            {"$limit": 1},
            {"$project": {etype+".id": 1}}
        ]
        r = list(self.aggregate_query(aggq))
        assert 1 == len(r)
        return uniprotid[0], r[0][etype]["id"]

    def kegg_drug_id_to_drugbank_id(self, keggdid):
        """
        Given KEGG drug id return Drugbank drug id
        :param keggdid: KEGG drug id
        :return: Drugbank drug id
        """
        project = {"external-identifiers": 1}
        qc = {"external-identifiers.identifier": keggdid}
        r = list(self.query(qc, projection=project))
        assert 1 == len(r)
        return r[0]["_id"]

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
    # based on file extension used, detailed in the readme.md file
    def get_connections_graph(self, qc, connections, outfile=None):
        interactions = self.get_connections(qc, connections)
        graph = nx.MultiDiGraph(name=connections, query=json.dumps(qc))
        colors = {
            "drug": 'yellowgreen',
            "targets": 'orchid',
            "enzymes": 'sienna',
            "transporters": 'coral',
            "carriers": 'blue'
        }
        _type = 'drug' if connections == 'drug-interactions' else connections
        for u, v in interactions:
            graph.add_node(u, type='drug', viz_color='green')
            graph.add_node(v,
                           type=_type,
                           viz_color=colors[_type])
            graph.add_edge(u, v)
        if outfile is not None:
            save_graph(graph, outfile)
        return graph

    def get_allgraphs(self, qc):
        connections = ["targets", "enzymes", "transporters", "carriers"]
        graphs = []
        for connection in connections:
            graphs.append(self.get_connections_graph(qc, connection))
        r = nx.compose_all(graphs)
        return r


class QueryHMDB:
    dbc = DBconnection(db, index)
    mdb = dbc.mdbi

    def getconnectedmetabolites(self, qc, max_associations=-1):
        # Return pairs of connected metabolites
        # together with associated proteins and their types
        graphlookup = True
        agpl = [
            {'$match': qc},
            {"$unwind": {
                "path": "$protein_associations.protein",
                "preserveNullAndEmptyArrays": False
            }},
            {'$graphLookup': {
                'from': DOCTYPE_PROTEIN,
                'startWith': '$protein_associations.protein.protein_accession',
                'connectFromField':
                    'metabolite_associations.metabolite.accession',
                'connectToField':
                    'accession',
                'maxDepth': 0,
                'depthField': "depth",
                'as': 'associated_proteins',
                "restrictSearchWithMatch": {
                    "metabolite_associations.metabolite.%d" % max_associations:
                        {"$exists": False}
                } if max_associations != -1 else {}
            }} if graphlookup else
            {'$lookup': {
                'from': DOCTYPE_PROTEIN,
                'foreignField':
                    'accession',
                'localField':
                    'protein_associations.protein.protein_accession',
                'as': 'associated_proteins'
            }},
            {'$project': {
                "name": 1,
                "protein_associations.protein.gene_name": 1,
                "protein_associations.protein.protein_type": 1,
                "associated_proteins.metabolite_associations.metabolite.name": 1
            }},
            {"$unwind": {
                "path": "$associated_proteins",
                "preserveNullAndEmptyArrays": False
             }},
            {'$match': {
                'associated_proteins.metabolite_associations.'
                'metabolite.%d' % max_associations: {
                    "$exists": False}} if max_associations != -1 else {}
             },
            {"$unwind": {
                "path": "$associated_proteins."
                        "metabolite_associations.metabolite",
                "preserveNullAndEmptyArrays": False
            }},
            {"$redact": {
                "$cond": [
                    {"$ne": [
                        "$name",
                        "$associated_proteins."
                        "metabolite_associations.metabolite.name"]},
                    "$$KEEP",
                    "$$PRUNE"]
            }},
            {'$group': {
                '_id': {
                    "m1": "$name",
                    "gene": "$protein_associations.protein.gene_name",
                    "type": "$protein_associations.protein.protein_type",
                    "m2": "$associated_proteins."
                          "metabolite_associations.metabolite.name"
                }
            }},
            {"$replaceRoot": {"newRoot": "$_id"}}
        ]
        r = self.mdb[DOCTYPE_METABOLITE].aggregate(agpl, allowDiskUse=True)
        return r

    def get_connections_graph(self, connections, query=None, outfile=None):
        graph = nx.DiGraph(name='HMDB', query=query)

        for i in connections:
            u = i['m1']
            v = i['m2']
            gene = i['gene']
            proteintype = i['type']
            graph.add_node(u, type='metabolite', viz_color='green')
            graph.add_node(v, type='metabolite', viz_color='honeydew')
            graph.add_node(gene, type=proteintype, viz_color='lavender')
            graph.add_edge(u, gene)
            graph.add_edge(gene, v)

        if outfile is not None:
            save_graph(graph, outfile)
        return graph


def savegraph(query, graphfile, connections='targets'):
    """Save DrugBank interactions as graph files
    :param query: MongoDB query clause to select subsets of DrugBank entries
                  ex: \'{"carriers.name": "Serum albumin"}\'
    :param graphfile: File name for saving the output graph
                    ' in GraphML, GML, Cytoscape.js or d3js formats,'
                    ' see readme.md for details'
    :param connections: "targets", "enzymes", "transporters" or
                              "carriers
    """
    from nosqlbiosets import parseinputquery
    qry = QueryDrugBank()
    qc = parseinputquery(query)
    g = qry.get_connections_graph(qc, connections, graphfile)
    print(nx.info(g))


def cyview(query, dataset='HMDB', connections='targets'):
    """ See HMDB/DrugBank graphs
     with Cytoscape runing on your local machine """
    from py2cytoscape.data.cyrest_client import CyRestClient
    from nosqlbiosets import parseinputquery
    qc = parseinputquery(query)
    if dataset == 'HMDB':
        qry = QueryHMDB()
        pairs = qry.getconnectedmetabolites(qc)
        mn = qry.get_connections_graph(pairs, query)
    else:  # assume DrugBank
        qry = QueryDrugBank()
        mn = qry.get_connections_graph(qc, connections)
    client = CyRestClient()
    client.network.create_from_networkx(mn)


if __name__ == '__main__':
    import argh
    argh.dispatch_commands([
        savegraph, cyview
    ])
