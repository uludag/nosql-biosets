#!/usr/bin/python3
# Index NCBI PubTator association files with Elasticsearch

import argparse
import gzip
import json

from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk

# Document type name for the Elascticsearch index entries
doctype = 'gene2pub'
ChunkSize = 2*1024


# Read given PubTator file, index using the index function specified
def read_and_index_pubtator_file(infile, es, indexfunc):
    if infile.endswith(".gz"):
        f = gzip.open(infile, 'rt')
    else:
        f = open(infile, 'r')
    r = indexfunc(es, f)
    return r


def parse_pub2gene_lines(f, r):
    for line in f:
        a = line.strip('\n').split('\t')
        if len(a) != 4:
            print("Line has more than 4 fields: %s" % line)
            exit(-1)
        r += 1
        if r > 1: # skip header line
            geneids = a[1].replace(';', ',').split(',')
            yield {
                '_id': r,
                "pmid": a[0], "geneid": geneids,
                "mentions": a[2], "resource": a[3]
            }


def es_index_links(es, f):
    r = 0
    for ok, result in streaming_bulk(
            es,
            parse_pub2gene_lines(f, r),
            index=args.index,
            doc_type='gene2pub',
            chunk_size=ChunkSize
    ):
        action, result = result.popitem()
        doc_id = '/%s/commits/%s' % (args.index, result['_id'])
        # process the information from ES whether the document has been
        # successfully indexed
        if not ok:
            print('Failed to %s document %s: %r' % (action, doc_id, result))
        #else:
        #    print(doc_id)
    return 1


def main(es, infile, index):
    #es.indices.delete(index=index, params={"timeout": "10s"})
    iconfig = json.load(open("./mappings/pubtator-es2.json", "rt"))
    es.indices.create(index=index, params={"timeout": "20s"},
                      ignore=400, body=iconfig, wait_for_active_shards=1)
    read_and_index_pubtator_file(infile, es, es_index_links)
    es.indices.refresh(index=index)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Index NCBI PubTator files using Elasticsearch')
    parser.add_argument('--infile',
                        default="./data/gene2pubtator.sample",
                        help='input file to index')
    parser.add_argument('--index',
                        default="pubtator-test1",
                        help='name of the Elasticsearch index')
    parser.add_argument('--host', default="localhost",
                        help='Elasticsearch server hostname')
    parser.add_argument('--port', default="9200",
                        help="Elasticsearch server port")
    args = parser.parse_args()
    host = args.host
    port = args.port
    con = Elasticsearch(host=host, port=port, timeout=3600)
    main(con, args.infile, args.index)
