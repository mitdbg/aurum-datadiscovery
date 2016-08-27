from elasticsearch import Elasticsearch
import config as c
from enum import Enum
from api.apiutils import Hit


class KWType(Enum):
    KW_TEXT = 0
    KW_SCHEMA = 1
    KW_ENTITIES = 2


class StoreHandler:

    # Store client
    client = None

    def __init__(self):
        """
            Uses the configuration file to create a connection to the store
            :return:
            """
        global client
        client = Elasticsearch([{'host': c.db_host, 'port': c.db_port}])

    def close(self):
        print("TODO")

    def get_all_text_fields(self):
        query_body = {"query": {"bool": {"filter": [{"term": {"dataType": "T"}}]}}}
        res = client.search(index='profile', body=query_body, scroll="10m",
                            filter_path=['_scroll_id',
                                         'hits.hits._id',
                                         'hits.total',
                                         'hits.hits._source.sourceName',
                                         'hits.hits._source.columnName',
                                         'hits.hits._source.totalValues',
                                         'hits.hits._source.uniqueValues']
                            )

        scroll_id = res['_scroll_id']
        remaining = res['hits']['total']
        while remaining > 0:
            hits = res['hits']['hits']
            for h in hits:
                id_source_and_file_name = (h['_id'], h['_source']['sourceName'], h['_source']['columnName'],
                                           h['_source']['totalValues'], h['_source']['uniqueValues'])
                yield id_source_and_file_name
                remaining -= 1
            res = client.scroll(scroll="3m", scroll_id=scroll_id,
                                filter_path=['_scroll_id',
                                             'hits.hits._id',
                                             'hits.hits._source.sourceName',
                                             'hits.hits._source.columnName',
                                             'hits.hits._source.totalValues',
                                             'hits.hits._source.uniqueValues']
                                )
            scroll_id = res['_scroll_id']  # update the scroll_id
        client.clear_scroll(scroll_id=scroll_id)

    def get_all_fields(self):
        """
        Reads all fields, described as (id, source_name, field_name) from the store.
        :return: a list of all fields with the form (id, source_name, field_name)
        """
        body = {"query": {"match_all": {}}}
        res = client.search(index='profile', body=body, scroll="10m",
                            filter_path=['_scroll_id',
                                         'hits.hits._id',
                                         'hits.total',
                                         'hits.hits._source.sourceName',
                                         'hits.hits._source.columnName',
                                         'hits.hits._source.totalValues',
                                         'hits.hits._source.uniqueValues']
                            )
        scroll_id = res['_scroll_id']
        remaining = res['hits']['total']
        while remaining > 0:
            hits = res['hits']['hits']
            for h in hits:
                id_source_and_file_name = (h['_id'], h['_source']['sourceName'], h['_source']['columnName'],
                                           h['_source']['totalValues'], h['_source']['uniqueValues'])
                yield id_source_and_file_name
                remaining -= 1
            res = client.scroll(scroll="3m", scroll_id=scroll_id,
                                filter_path=['_scroll_id',
                                             'hits.hits._id',
                                             'hits.hits._source.sourceName',
                                             'hits.hits._source.columnName',
                                             'hits.hits._source.totalValues',
                                             'hits.hits._source.uniqueValues']
                                )
            scroll_id = res['_scroll_id']  # update the scroll_id
        client.clear_scroll(scroll_id=scroll_id)

    def get_fields_text_index(self):
        """
        Reads all fields, described as (id, source_name, field_name) from the store (text index).
        :return: a list of all fields with the form (id, source_name, field_name)
        """
        body = {"query": {"match_all": {}}}
        res = client.search(index='text', body=body, scroll="10m",
                            filter_path=['_scroll_id',
                                         'hits.hits._id',
                                         'hits.total',
                                         'hits.hits._source.id',
                                         'hits.hits._source.sourceName',
                                         'hits.hits._source.columnName']
                            )
        scroll_id = res['_scroll_id']
        remaining = res['hits']['total']
        while remaining > 0:
            hits = res['hits']['hits']
            for h in hits:
                rawid_id_source_and_file_name = (h['_id'], h['_source']['id'],
                                                 h['_source']['sourceName'], h['_source']['columnName'])
                yield rawid_id_source_and_file_name
                remaining -= 1
            res = client.scroll(scroll="3m", scroll_id=scroll_id,
                                filter_path=['_scroll_id',
                                             'hits.hits._id',
                                             'hits.hits._source.id',
                                             'hits.hits._source.sourceName',
                                             'hits.hits._source.columnName']
                                )
            scroll_id = res['_scroll_id']  # update the scroll_id
        client.clear_scroll(scroll_id=scroll_id)

    def get_all_fields_of_source(self, source_name):
        body = {"query": {"match": {"sourceName": source_name}}}
        res = client.search(index='profile', body=body, scroll="10m",
                            filter_path=['_scroll_id',
                                         'hits.hits._id',
                                         'hits.total',
                                         'hits.hits._source.sourceName',
                                         'hits.hits._source.columnName']
                            )
        scroll_id = res['_scroll_id']
        remaining = res['hits']['total']
        while remaining > 0:
            hits = res['hits']['hits']
            for h in hits:
                hit = Hit(h['_id'], h['_source']['sourceName'], h['_source']['columnName'], -1)
                yield hit
                remaining -= 1
            res = client.scroll(scroll="3m", scroll_id=scroll_id,
                                filter_path=['_scroll_id',
                                             'hits.hits._id',
                                             'hits.hits._source.sourceName',
                                             'hits.hits._source.columnName']
                                )
            scroll_id = res['_scroll_id']  # update the scroll_id
        client.clear_scroll(scroll_id=scroll_id)

    def get_all_fields_with(self, attrs):
        """
        Reads all fields, described as (id, source_name, field_name) from the store.
        :return: a list of all fields with the form (id, source_name, field_name)
        """
        template = 'hits.hits._source.'
        filter_path = ['_scroll_id',
                       'hits.hits._id',
                       'hits.total',
                       'hits.hits._source.sourceName',
                       'hits.hits._source.columnName']
        for attr in attrs:
            new_filter_path = template + attr
            filter_path.append(new_filter_path)

        body = {"query": {"match_all": {}}}
        res = client.search(index='profile', body=body, scroll="10m",
                            filter_path=filter_path
                            )
        scroll_id = res['_scroll_id']
        remaining = res['hits']['total']
        while remaining > 0:
            hits = res['hits']['hits']
            for h in hits:
                toret = []
                toret.append(h['_id'])
                toret.append(h['_source']['sourceName'])
                toret.append(h['_source']['columnName'])
                for attr in attrs:
                    toret.append(h['_source'][attr])
                tuple_result = tuple(toret)
                yield tuple_result
                remaining -= 1
            res = client.scroll(scroll="3m", scroll_id=scroll_id,
                                filter_path=filter_path
                                )
            scroll_id = res['_scroll_id']  # update the scroll_id
        client.clear_scroll(scroll_id=scroll_id)

    def peek_values(self, field, num_values):
        """
        Reads sample values for the given field
        :param field: The field from which to read values
        :param num_values: The number of values to read
        :return: A list with the sample values read for field
        """
        print("TODO")

    def search_keywords(self, keywords, elasticfieldname, max_hits=15):
        """
        Performs a search query on elastic_field_name to match the provided keywords
        :param keywords: the list of keyword to match
        :param elasticfieldname: what is the field in the store where to apply the query
        :return: the list of documents that contain the keywords
        """
        index = None
        query_body = None
        filter_path = ['hits.hits._source.id',
                       'hits.hits._score',
                       'hits.total',
                       'hits.hits._source.sourceName',
                       'hits.hits._source.columnName']
        if elasticfieldname == KWType.KW_TEXT:
            index = "text"
            query_body = {"from": 0, "size": max_hits, "query": {"match": {"text": keywords}}}
        elif elasticfieldname == KWType.KW_SCHEMA:
            index = "profile"
            query_body = {"from": 0, "size": max_hits, "query": {"match": {"columnName": keywords}}}
        elif elasticfieldname == KWType.KW_ENTITIES:
            index = "profile"
            query_body = {"from": 0, "size": max_hits, "query": {"match": {"entities": keywords}}}
        res = client.search(index=index, body=query_body, filter_path=filter_path)
        if res['hits']['total'] == 0:
            return []
        for el in res['hits']['hits']:
            data = Hit(el['_source']['id'], el['_source']['sourceName'], el['_source']['columnName'], el['_score'])
            yield data

    def get_all_fields_entities(self):
        """
        Retrieves all fields and entities from the store
        :return: (fields, entities)
        """
        results = self.get_all_fields_with(['entities'])
        fields = []
        ents = []
        for r in results:
            (nid, sn, fn, entities) = r
            fields.append((nid, sn, fn))
            ents.append(entities)
        return fields, ents

    def get_all_docs_from_text_with_idx_id(self, doc_id):
        """
        Reads all fields, described as (id, source_name, field_name) from the store.
        :return: a list of all fields with the form (id, source_name, field_name)
        """
        body = {"query": {"bool": {"must": [{"match": {"id": doc_id}}]}}}
        res = client.search(index='text', body=body, scroll="10m",
                            filter_path=['_scroll_id',
                                         'hits.hits._id',
                                         'hits.total',
                                         'hits.hits._source.sourceName',
                                         'hits.hits._source.columnName'
                                         ]
                            )
        scroll_id = res['_scroll_id']
        remaining = res['hits']['total']
        while remaining > 0:
            hits = res['hits']['hits']
            for h in hits:
                raw_id_doc = h['_id']
                yield raw_id_doc
                remaining -= 1
            res = client.scroll(scroll="3m", scroll_id=scroll_id,
                                filter_path=['_scroll_id',
                                             'hits.hits._id',
                                             'hits.hits._source.id']
                                )
            scroll_id = res['_scroll_id']  # update the scroll_id
        client.clear_scroll(scroll_id=scroll_id)

    def get_all_fields_textsignatures(self):
        # FIXME: still need to add filter for terms that appear less than once, or add freq to the vector of tersm
        # using the map value and then post filter in a better way

        def partition_ids(ids, partition_size=50):
            for i in range(0, len(ids), partition_size):
                yield ids[i:i + partition_size]

        fields = []
        text_signatures = []

        #term_body = {"filter": {"min_term_freq": 2, "max_num_terms": c.sig_v_size}}
        # We get the ids from 'profile'
        text_fields_gen = self.get_all_text_fields()
        for (uid, sn, fn, tv, uv) in text_fields_gen:
            # We retrieve all documents indexed with the same id in 'text'
            docs = self.get_all_docs_from_text_with_idx_id(uid)
            ids = [x for x in docs]
            ids_partitions = partition_ids(ids)  # partition ids so that they fit in one http request
            all_terms = dict()
            for partition in ids_partitions:
                # We get the term vectors for each group of those documents
                ans = client.mtermvectors(index='text', ids=partition, doc_type='column')#, body=term_body)

                # We merge them somehow
                found_docs = ans['docs']
                for doc in found_docs:
                    term_vectors = doc['term_vectors']
                    if 'text' in term_vectors:
                        terms = list(term_vectors['text']['terms'].keys())
                        for t in terms:
                            all_terms[t] = 0  # we don't care about the value
            fields.append((uid, sn, fn))
            text_signatures.append(list(all_terms.keys()))
        return fields, text_signatures

    def _get_all_fields_textsignatures(self):
        """
        Retrieves textual fields and signatures from the store
        :return: (fields, textsignatures)
        """
        term_body = {"filter": {"min_term_freq": 2, "max_num_terms": c.sig_v_size}}
        fields = []
        seen_nid = []
        text_signatures = []
        text_fields_gen = self.get_fields_text_index()
        for (rawid, nid, sn, fn) in text_fields_gen:
            if nid not in seen_nid:

                ans = client.termvectors(index='text', id=rawid, doc_type='column', body=term_body)
                terms = []
                if ans['found']:
                    term_vectors = ans['term_vectors']
                    if 'text' in term_vectors:
                        terms = list(ans['term_vectors']['text']['terms'].keys())
                # Note that we filter out fields for which we don't get terms
                # This can be due to empty source data, or noisy data with all-stopwords, etc.

                if len(terms) > 0:
                    fields.append((nid, sn, fn))
                    text_signatures.append(terms)
                    #field = (nid, sn, fn)
                    #yield (field, terms)
            seen_nid.append(nid)
        return fields, text_signatures

    def get_all_fields_numsignatures(self):
        """
        Retrieves numerical fields and signatures from the store
        :return: (fields, numsignatures)
        """
        query_body = {"query": {"bool": {"filter": [{"term": {"dataType": "N"}}]}}}
        res = client.search(index='profile', body=query_body, scroll="10m",
                            filter_path=['_scroll_id',
                                         'hits.hits._id',
                                         'hits.total',
                                         'hits.hits._source.sourceName',
                                         'hits.hits._source.columnName',
                                         'hits.hits._source.median',
                                         'hits.hits._source.iqr']
                            )
        scroll_id = res['_scroll_id']
        remaining = res['hits']['total']
        fields = []
        num_sig = []
        while remaining > 0:
            hits = res['hits']['hits']
            for h in hits:
                id_source_and_file_name = (h['_id'], h['_source']['sourceName'], h['_source']['columnName'])
                fields.append(id_source_and_file_name)
                num_sig.append((h['_source']['median'], h['_source']['iqr']))
                remaining -= 1
            res = client.scroll(scroll="3m", scroll_id=scroll_id,
                                filter_path=['_scroll_id',
                                             'hits.hits._id',
                                             'hits.hits._source.sourceName',
                                             'hits.hits._source.columnName',
                                             'hits.hits._source.median',
                                             'hits.hits._source.iqr']
                                )
            scroll_id = res['_scroll_id']  # update the scroll_id
        client.clear_scroll(scroll_id=scroll_id)
        return fields, num_sig


if __name__ == "__main__":
    print("Elastic Store")
    handler = StoreHandler()
    all_fields = handler.get_all_fields()
    total = [x for x in all_fields]
    print("Total fields: " + str(len(total)))

    data = handler.get_all_fields_textsignatures()
    fields, text_sig = data
    all_text = [x for x in fields]
    print("Text fields: " + str(len(all_text)))

    data = handler.get_all_fields_numsignatures()
    fields, num_sig = data
    all_num = [x for x in fields]
    print("Num fields: " + str(len(all_num)))
