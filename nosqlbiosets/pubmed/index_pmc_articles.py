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
import datetime

import pubmed_parser as pp

from nosqlbiosets.dbutils import DBconnection

PMCARTICLE = 'PMC_article'
d = os.path.dirname(os.path.abspath(__file__))


# Read PMC article xml files, index using the function indexf
# If the input file is a folder iterate over files in the folder
def read_and_index_pmc_articles(infile, dbc, indexf):
    print("Reading %s " % infile)
    n = 0
    t1 = time.time()
    if os.path.isdir(infile):
        for child in os.listdir(infile):
            c = os.path.join(infile, child)
            read_and_index_pmc_articles_file(c, dbc, indexf)
            n += 1
    else:
        if infile.endswith(".tar"):
            n = read_and_index_pmc_articles_tarfile(infile, dbc, indexf)
        else:
            read_and_index_pmc_articles_file(infile, dbc, indexf)
            n = 1
    t2 = time.time()
    print("-- %d files have been processed, in %dms"
          % (n, (t2 - t1) * 1000))
    return None


def pubmed_parser_parse(path_xml):
    ar = pp.parse_pubmed_xml(path_xml)
    # if dbc.db != "Elasticsearch" or \
    #         not dbc.es.exists(index=dbc.index, doc_type=PMCARTICLE,
    #                           id=ar['pmid']):
    path_xml.seek(0)
    paragraph_dicts = pp.parse_pubmed_paragraph(path_xml)
    paragraphs = []
    for p in paragraph_dicts:
        del (p['pmc'])
        del (p['pmid'])
        paragraphs.append(p)
    ar['paragraphs'] = paragraphs
    ar['pub_date'] = datetime.datetime(int(ar['publication_year']), 1, 1)
    return ar


def xmltodict_parser_parse(path_xml):
    import xmltodict
    namespaces = {
        'http://www.w3.org/1999/xlink': None,
        'http://www.w3.org/1998/Math/MathML': None
    }
    ar = xmltodict.parse(path_xml, process_namespaces=True,
                         namespaces=namespaces, attr_prefix='')['article']
    # Delete attributes we could not handle
    if 'glossary' in ar['back']:
        del ar['back']['glossary']
    del ar['back']['ref-list']
    return ar


PARSER = pubmed_parser_parse


# Read given PMC tar file, index using the index function specified
def read_and_index_pmc_articles_tarfile(infile, dbc, indexfunc):
    print("\nProcessing tar file: %s " % infile)
    i = 0
    tar = tarfile.open(infile, 'r:')
    for member in tar:
        f = tar.extractfile(member)
        if f is None:
            continue  # if the tar-file entry is folder then skip
        ar = PARSER(f)
        indexfunc(dbc, ar)
        tar.members = []
        i += 1
    return i


# Read PMC articles file, index using the function indexf
def read_and_index_pmc_articles_file(infile_, dbc, indexf):
    infile = str(infile_)
    print("Reading %s " % infile)
    if infile.endswith(".gz"):
        f = gzip.open(infile, 'rb')
    else:
        f = open(infile, 'rb')
    ba = PARSER(f)
    r = indexf(dbc, ba)
    return r


def index_article(dbc, ar):
    print(".", end='', file=sys.stdout)
    sys.stdout.flush()
    if PARSER == pubmed_parser_parse:
        pmid = ar['pmid']
    else:
        pmid = ar['front']['article-meta']['article-id'][0]['#text']
    try:
        if dbc.db == "Elasticsearch":
            dbc.es.index(index=dbc.index, doc_type=PMCARTICLE, id=pmid,
                         body=ar)
        else:  # MongoDB
            spec = {"_id": pmid}
            dbc.mdbi[PMCARTICLE].update(spec, ar, upsert=True)
    except Exception as e:
        print("error: %s" % e)
    return None


def main(infile, db, index, host, port):
    iconfig = json.load(open(d+"/../../mappings/pmc-articles.json", "r"))
    dbc = DBconnection(db, index, host, port,
                       es_indexmappings=iconfig['mappings'])
    read_and_index_pmc_articles(infile, dbc, index_article)
    dbc.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Index PMC articles, using Elasticsearch or MongoDB')
    parser.add_argument('-infile', '--infile',
                        default=d + "/../../data/PMC0044-1000.tar",
                        help='input file; tar file ???? '
                             'or folder with PMC files')
    parser.add_argument('--index',
                        default="biosets",
                        help='Name of the Elasticsearch index'
                             ' or MongoDB database')
    parser.add_argument('--host',
                        help='Elasticsearch or MongoDB server hostname')
    parser.add_argument('--port',
                        help="Elasticsearch or MongoDB server port number")
    parser.add_argument('--db', default='Elasticsearch',
                        help="Database: Elasticsearch or MongoDB")
    args = parser.parse_args()
    main(args.infile, args.db, args.index, args.host, args.port)
