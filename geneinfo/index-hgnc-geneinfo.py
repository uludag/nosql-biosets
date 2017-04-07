#!/usr/bin/env python
# Index HGNC gene info files with Elasticsearch

import argparse
import gzip
import json

from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk

# Document type name for the Elascticsearch index entries
doctype = 'coding-gene'
ChunkSize = 2*1024


# Read protein-coding-genes file, index using the index function specified
def read_and_index_hgnc_file(infile, es, indexfunc):
    if infile.endswith(".gz"):
        f = gzip.open(infile, 'rt')
    else:
        f = open(infile, 'r')
    l = json.load(f)
    r = indexfunc(es, l["response"])
    return r


def iterate_over_genes(l):
    for gene in l['docs']:
        if 'pseudogene.org' in gene:
            gene['pseudogene_dot_org_id'] = gene['pseudogene.org']
            del(gene['pseudogene.org'])
        yield gene


def es_index_genes(es, l):
    r = 0
    for ok, result in streaming_bulk(
            es,
            iterate_over_genes(l),
            index=args.index,
            doc_type=doctype,
            chunk_size=ChunkSize
    ):
        action, result = result.popitem()
        doc_id = '/%s/commits/%s' % (args.index, result['_id'])
        if not ok:
            print('Failed to %s document %s: %r' % (action, doc_id, result))
        else:
            r += 1
    return r


def main(es, infile, index):
    # es.indices.delete(index=index, params={"timeout": "10s"})
    # iconfig = json.load(open("./mappings/protein-coding-genes.json", "rt"))
    es.indices.create(index=index, params={"timeout": "20s"},
                      ignore=400,  # body=iconfig,
                      wait_for_active_shards=1)
    read_and_index_hgnc_file(infile, es, es_index_genes)
    es.indices.refresh(index=index)


if __name__ == '__main__':
    conf = {"host": "localhost", "port": 9200}
    try:
        conf = json.load(open("../conf/elasticsearch.json", "rt"))
    finally:
        pass
    parser = argparse.ArgumentParser(
        description='Index HGNC gene-info files using Elasticsearch')
    parser.add_argument('--infile',
                        default="./data/protein-coding_gene.json",
                        help='input file to index')
    parser.add_argument('--index',
                        default="hgnc-geneinfo",
                        help='Elasticsearch index')
    parser.add_argument('--host', default=conf['host'],
                        help='Elasticsearch server hostname')
    parser.add_argument('--port', default=conf['port'],
                        help="Elasticsearch server port")
    args = parser.parse_args()
    host = args.host
    port = args.port
    con = Elasticsearch(host=host, port=port, timeout=3600)
    main(con, args.infile, args.index)
