#!/usr/bin/env python
""" Index PubmedArticleSet XML documents with Elasticsearch or MongoDB """
import argparse
import gzip
import os
import time
import datetime
import pubmed_parser as pp
from elasticsearch.helpers import parallel_bulk
from nosqlbiosets.dbutils import DBconnection, dbargs
from nosqlbiosets.objutils import num

SOURCEURL = "ftp://ftp.ncbi.nlm.nih.gov/pubmed/baseline/"
d = os.path.dirname(os.path.abspath(__file__))


class IndexPubMedArticles(DBconnection):

    def __init__(self, db, index, **kwargs):
        super(IndexPubMedArticles, self).__init__(db, index, **kwargs)
        if 'mdbcollection' in kwargs:
            self.mdbcollection = kwargs['mdbcollection']

    # If the input file is a folder iterate over files in the folder
    def read_and_index_articles(self, infile):
        n = 0
        t1 = time.time()
        if os.path.isdir(infile):
            for child in os.listdir(infile):
                c = os.path.join(infile, child)
                self.read_and_index_articles_file(c)
                n += 1
        else:
            self.read_and_index_articles_file(infile)
            n = 1
        t2 = time.time()
        print("-- %d files have been processed, in %ds"
              % (n, (t2 - t1)))

    def read_and_index_articles_file(self, infile_):
        infile = str(infile_)
        print("Reading %s " % infile)
        if infile.endswith(".xml.gz"):
            f = gzip.open(infile, 'rb')
        elif infile.endswith(".xml"):
            f = open(infile, 'rb')
        else:
            print("ERROR: filename does not end with '.xml' or '.xml.gz' '%s'"
                  % infile)
            return
        articles = pp.parse_medline_xml(f)
        listattrs = ['authors', 'mesh_terms', 'publication_types',
                     'chemical_list', 'keywords', 'references', 'affiliations'
                     ]
        for ar in articles:
            num(ar, 'pmc')
            ar['_id'] = num(ar, 'pmid')
            ar['pubdate'] = datetime.datetime(int(ar['pubdate']), 1, 1)
            for listattr in listattrs:
                if len(ar[listattr]) == 0:
                    del ar[listattr]
                else:
                    spr = ';' if listattr in ['authors', 'references'] else '; '
                    ar[listattr] = ar[listattr].split(spr)

        if self.db == "Elasticsearch":
            self.es_index(articles)
        else:  # assume MongoDB
            self.mdb_index(articles)

    def es_index(self, articles):
        for ok, result in parallel_bulk(
                self.es, iter(articles),
                thread_count=14, queue_size=1400,
                index=self.index, chunk_size=140
        ):
            if not ok:
                action, result = result.popitem()
                doc_id = '/%s/commits/%s' % (self.index, result['_id'])
                print('Failed to %s document %s: %r' % (action, doc_id, 'result'))
        return

    def mdb_index(self, ar):
        try:
            self.mdbi[self.mdbcollection].insert_many(ar)
        except Exception as e:
            print("***** ERROR: %s" % e)


def main(infile, db, index, **kwargs):
    dbc = IndexPubMedArticles(db, index, **kwargs)
    dbc.read_and_index_articles(infile)
    dbc.close()


if __name__ == '__main__':
    args = argparse.ArgumentParser(
        description='Index PubmedArticleSet XML document files with Elasticsearch,'
                    ' or MongoDB, downloaded from ' + SOURCEURL)
    args.add_argument('-infile', '--infile',
                      help='PubmedArticleSet XML document file,'
                           ' such as pubmed20n0124.xml.gz'
                           ' or input folder with the XML document files')
    dbargs(args)
    args = args.parse_args()
    main(args.infile, args.dbtype, args.esindex, host=args.host, port=args.port)
