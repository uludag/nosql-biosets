#!/usr/bin/env python
""" Index PMC articles with Elasticsearch or MongoDB"""
from __future__ import print_function

import argparse
import gzip
import json
import os
import sys
import tarfile
import time

import pubmed_parser as pp

from nosqlbiosets.dbutils import DBconnection

PMCARTICLE = 'PMC_article'


# Read PMC article xml files, index using the function indexf
# If the input file is a folder iterate over files in the folder
def read_and_index_pmc_articles(infile, es, indexf):
    print("Reading %s " % infile)
    n = 0
    t1 = time.time()
    if os.path.isdir(infile):
        for child in os.listdir(infile):
            c = os.path.join(infile, child)
            read_and_index_pmc_articles_file(c, es, indexf)
            n += 1
    else:
        if infile.endswith(".tar"):
            n = read_and_index_pmc_articles_tarfile(infile, es, indexf)
        else:
            read_and_index_pmc_articles_file(infile, es, indexf)
            n = 1
    t2 = time.time()
    print("-- %d files have been processed, in %dms"
          % (n, (t2 - t1) * 1000))
    return None


# Read given PMC tar file, index using the index function specified
def read_and_index_pmc_articles_tarfile(infile, dbc, indexfunc):
    print("\nProcessing tar file: %s " % infile)
    i = 0
    tar = tarfile.open(infile, 'r:')
    for member in tar:
        f = tar.extractfile(member)
        if f is None:
            continue  # if the tar-file entry is folder then skip

        ar = pp.parse_pubmed_xml(f)
        if dbc.db != "Elasticsearch" or \
                not dbc.es.exists(index=args.index, doc_type=PMCARTICLE,
                                  id=ar['pmid']):
            try:
                # We here open the file for the second time, can be improved?
                f = tar.extractfile(member)
                paragraph_dicts = pp.parse_pubmed_paragraph(f)
                paragraphs = []
                for p in paragraph_dicts:
                    del (p['pmc'])
                    del (p['pmid'])
                    paragraphs.append(p)
                import datetime
                ar['paragraphs'] = paragraphs
                ar['pub_date'] = datetime.datetime(int(ar['publication_year']),
                                                   1, 1)
                indexfunc(dbc, ar)
                tar.members = []
                i += 1
            except Exception as e:
                print('Error: %s' % e)
                exit(-1)
        else:
            print("-", end='', flush=True)
    return i


def pubmed_parser_parse(path_xml):
    pubmed_dict = pp.parse_pubmed_xml(path_xml)
    # pubmed_dict = pp.parse_pubmed_paragraph(path_xml)
    # print(json.dumps(pubmed_dict, indent=2))
    return pubmed_dict


# Read PMC articles file, index using the function indexf
def read_and_index_pmc_articles_file(infile_, es, indexf):
    infile = str(infile_)
    print("Reading %s " % infile)
    if infile.endswith(".gz"):
        f = gzip.open(infile, 'rt')
    else:
        f = open(infile, 'r')
    ba = pubmed_parser_parse(f)
    r = indexf(es, ba)
    return r


def index_article(dbc, ar):
    print(".", end='', file=sys.stdout)
    sys.stdout.flush()

    pmid = ar['pmid']
    try:
        if dbc.db == "Elasticsearch":
            dbc.es.index(index=args.index, doc_type=PMCARTICLE, id=pmid,
                         body=ar)
        else:  # MongoDB
            spec = {"_id": pmid}
            dbc.mdbi[PMCARTICLE].update(spec, ar, upsert=True)
    except Exception as e:
        print("error: %s" % e)
    return None


if __name__ == '__main__':
    d = os.path.dirname(os.path.abspath(__file__))
    parser = argparse.ArgumentParser(
        description='Index PMC articles, using Elasticsearch')
    parser.add_argument('-infile', '--infile',
                        default=d + "/../../data/PMC0044-1000.tar",
                        help='input file name or folder with PMC files')
    parser.add_argument('--index',
                        default="tests",
                        help='Name of the Elasticsearch index')
    parser.add_argument('--host',
                        help='Elasticsearch server hostname')
    parser.add_argument('--port',
                        help="Elasticsearch server port")
    parser.add_argument('--db', default='Elasticsearch',
                        help="Database: Elasticsearch or MongoDB")
    args = parser.parse_args()
    iconfig = json.load(open(d+"/../../mappings/pmc-articles.json", "r"))
    dbcon = DBconnection(args.db, args.index, args.host, args.port,
                         es_indexmappings=iconfig['mappings'])
    read_and_index_pmc_articles(args.infile, dbcon, index_article)
    dbcon.close()
