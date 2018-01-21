#!/usr/bin/env python
""" Test queries with HMDB metabolites and proteins """
import unittest

from hmdb.index import DOCTYPE_METABOLITE, DOCTYPE_PROTEIN
from nosqlbiosets.dbutils import DBconnection


class QueryHMDB(unittest.TestCase):
    index = "biosets"
    db = "MongoDB"
    dbc = DBconnection(db, index)
    mdb = dbc.mdbi

    def query(self, qc, doctype=None, size=20):
        print(self.db)
        print("Querying '%s' records with clause '%s'" % (doctype, str(qc)))
        c = self.mdb[doctype].find(qc, limit=size)
        r = [doc for doc in c]
        c.close()
        return r

    def test_ex_keggids_query(self):
        keggids = ['C00473']
        if self.dbc.db == 'MongoDB':
            qc = {"kegg_id": ' '.join(keggids)}
            hits = self.query(qc, DOCTYPE_METABOLITE)
            hmdbids = [c['_id'] for c in hits]
            self.assertEqual(hmdbids, ['HMDB0000305'])

    def test_ex_text_search(self):
        qterms = ['ATP']
        qc = {'$text': {'$search': ' '.join(qterms)}}
        hits = self.query(qc, DOCTYPE_METABOLITE)
        mids = [c['_id'] for c in hits]
        self.assertEqual(len(mids), 20)

    def test_ex_aggregate_query(self):
        agpl = [
            {'$match': {'$text': {'$search': 'bacteriocin'}}},
            {'$group': {
                '_id': '$taxonomy.super_class', "count": {"$sum": 1}}}
        ]
        hits = self.mdb[DOCTYPE_METABOLITE].aggregate(agpl)
        mids = [c['_id'] for c in hits]
        self.assertIn('Organoheterocyclic compounds', mids)

    @unittest.skipIf(db == "Elasticsearch", "Elasticsearch support not yet"
                                            " implemented")
    def test_ex_lookup_with_metabolite_ids(self):
        agpl = [
            {'$match': {'$text': {'$search': 'antibiotic'}}},
            {'$match': {
                "ontology.cellular_locations.cellular_location": "Cytoplasm"}},
            {'$lookup': {
                'from': DOCTYPE_PROTEIN,
                'localField': 'accession',
                'foreignField': 'metabolite_associations.metabolite.accession',
                'as': 'protein_docs'
            }},
            {"$match": {
                "protein_docs.10": {"$exists": True}}}
        ]
        hits = list(self.mdb[DOCTYPE_METABOLITE].aggregate(agpl))
        assert len(hits) > 0
        assert len(hits[0]['protein_docs']) >= 10

    def test_ex_lookup_with_gene_names(self):
        agpl = [
            {'$match': {"name": "Succinic acid semialdehyde"}},
            {'$unwind':
                {
                    'path': '$protein_associations.protein'
                }},
            {'$project': {
                "accession": 1,
                "protein_associations.protein": 1}},
            {'$lookup': {
                'from': DOCTYPE_PROTEIN,
                'localField':
                    'protein_associations.protein.gene_name',
                'foreignField': 'gene_name',
                'as': 'proteins'
            }},
            {'$project': {
                "accession": 1,
                "protein_associations.protein": 1,
                "proteins.general_function": 1
            }}
        ]
        hits = self.mdb[DOCTYPE_METABOLITE].aggregate(agpl)
        proteintypes = (
            c["protein_associations"]["protein"]["protein_type"] for c in hits
        )
        hits.close()
        self.assertIn("Enzyme", proteintypes)


if __name__ == '__main__':
    unittest.main()
