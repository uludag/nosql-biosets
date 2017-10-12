#!/usr/bin/env python
# Index HGNC gene info files with Elasticsearch

import argparse
import gzip
import json
from pprint import pprint

from elasticsearch.helpers import streaming_bulk
from pymongo.errors import BulkWriteError

from nosqlbiosets.dbutils import DBconnection


# Document type name for Elascticsearch index entries,
# and collection name with MongoDB
DOCTYPE = 'hgncgeneinfo'
INDEX = "geneinfo"
CHUNKSIZE = 64


# Read protein-coding-genes file, index using the index function specified
def read_and_index_hgnc_file(infile, es, indexfunc):
    if infile.endswith(".gz"):
        f = gzip.open(infile, 'rt')
    else:
        f = open(infile, 'r')
    genesinfo = json.load(f)
    r = indexfunc(es, genesinfo["response"])
    return r


def read_genes(l):
    for gene in l['docs']:
        if 'pseudogene.org' in gene:
            gene['pseudogene_dot_org_id'] = gene['pseudogene.org']
            del(gene['pseudogene.org'])
        gene["_id"] = int(gene["hgnc_id"][5:])  # skip prefix "HGNC:"
        yield gene


def es_index_genes(es, genes):
    r = 0
    for ok, result in streaming_bulk(
            es,
            read_genes(genes),
            index=args.index,
            doc_type=DOCTYPE,
            chunk_size=CHUNKSIZE
    ):
        action, result = result.popitem()
        doc_id = '/%s/commits/%s' % (args.index, result['_id'])
        if not ok:
            print('Failed to %s document %s: %r' % (action, doc_id, result))
        else:
            r += 1
    return r


def mongodb_index_genes(mdbi, genes):
    entries = list()
    mdbi[DOCTYPE].delete_many({})
    try:
        for entry in read_genes(genes):
            entries.append(entry)
            if len(entries) == CHUNKSIZE:
                mdbi[DOCTYPE].insert_many(entries)
                entries = list()
        mdbi[DOCTYPE].insert_many(entries)
    except BulkWriteError as bwe:
        pprint(bwe.details)
    return


def main(dbc, infile, index):
    if dbc.db == "Elasticsearch":
        read_and_index_hgnc_file(infile, dbc.es, es_index_genes)
        dbc.es.indices.refresh(index=index)
    else:
        read_and_index_hgnc_file(infile, dbc.mdbi, mongodb_index_genes)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Index HGNC gene-info files using Elasticsearch or MongoDB')
    parser.add_argument('--infile',
                        # required=True,
                        default="./data/hgnc_complete_set.json",
                        help='input file to index')
    parser.add_argument('--index', default=INDEX,
                        help='Elasticsearch index')
    parser.add_argument('--host',
                        help='Elasticsearch server hostname')
    parser.add_argument('--port',
                        help="Elasticsearch server port")
    parser.add_argument('--db', default='MongoDB',
                        help="Database: 'Elasticsearch' or 'MongoDB'")
    args = parser.parse_args()
    dbc_ = DBconnection(args.db, args.index, host=args.host, port=args.port)
    main(dbc_, args.infile, args.index)
