import re
from elasticsearch import Elasticsearch

from enum import Enum
from collections import defaultdict

from api.apiutils import Hit
import config as c


class KWType(Enum):
    KW_TEXT = 0
    KW_SCHEMA = 1
    KW_ENTITIES = 2
    KW_TABLE = 3


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
                                         'hits.hits._source.dbName',
                                         'hits.hits._source.sourceName',
                                         'hits.hits._source.columnName',
                                         'hits.hits._source.totalValues',
                                         'hits.hits._source.uniqueValues',
                                         'hits.hits._source.dataType']
                            )
        scroll_id = res['_scroll_id']
        remaining = res['hits']['total']
        while remaining > 0:
            hits = res['hits']['hits']
            for h in hits:
                id_source_and_file_name = (h['_id'], h['_source']['dbName'], h['_source']['sourceName'],
                                           h['_source']['columnName'], h['_source']['totalValues'],
                                           h['_source']['uniqueValues'], h['_source']['dataType'])
                yield id_source_and_file_name
                remaining -= 1
            res = client.scroll(scroll="5m", scroll_id=scroll_id,
                                filter_path=['_scroll_id',
                                             'hits.hits._id',
                                             'hits.hits._source.dbName',
                                             'hits.hits._source.sourceName',
                                             'hits.hits._source.columnName',
                                             'hits.hits._source.totalValues',
                                             'hits.hits._source.uniqueValues',
                                             'hits.hits._source.dataType']
                                )
            scroll_id = res['_scroll_id']  # update the scroll_id
        client.clear_scroll(scroll_id=scroll_id)

    def get_all_fields_with(self, attrs):
        # FIXME: this function was not updated after 2 refactoring processes.
        """
        Reads all fields, described as (id, source_name, field_name) from the store.
        :return: a list of all fields with the form (id, source_name, field_name)
        """
        template = 'hits.hits._source.'
        filter_path = ['_scroll_id',
                       'hits.hits._id',
                       'hits.total',
                       'hits.hits._source.dbName',
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
            res = client.scroll(scroll="5m", scroll_id=scroll_id,
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
                       'hits.hits._source.dbName',
                       'hits.hits._source.sourceName',
                       'hits.hits._source.columnName']
        if elasticfieldname == KWType.KW_TEXT:
            index = "text"
            query_body = {"from": 0, "size": max_hits,
                          "query": {"match": {"text": keywords}}}
        elif elasticfieldname == KWType.KW_SCHEMA:
            index = "profile"
            query_body = {"from": 0, "size": max_hits,
                          "query": {"match": {"columnName": keywords}}}
        elif elasticfieldname == KWType.KW_ENTITIES:
            index = "profile"
            query_body = {"from": 0, "size": max_hits,
                          "query": {"match": {"entities": keywords}}}
        elif elasticfieldname == KWType.KW_TABLE:
            index = "profile"
            query_body = {"from": 0, "size": max_hits,
                          "query": {"match": {"sourceName": keywords}}}
        res = client.search(index=index, body=query_body,
                            filter_path=filter_path)
        if res['hits']['total'] == 0:
            return []
        for el in res['hits']['hits']:
            data = Hit(el['_source']['id'], el['_source']['dbName'], el['_source']['sourceName'],
                       el['_source']['columnName'], el['_score'])
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
                                         'hits.total'
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
            res = client.scroll(scroll="5m", scroll_id=scroll_id,
                                filter_path=['_scroll_id',
                                             'hits.hits._id',
                                             'hits.hits._source.id']
                                )
            scroll_id = res['_scroll_id']  # update the scroll_id
        client.clear_scroll(scroll_id=scroll_id)

    def get_all_fields_text_signatures(self, network):

        def partition_ids(ids, partition_size=50):
            for i in range(0, len(ids), partition_size):
                yield ids[i:i + partition_size]

        def filter_term_vector_by_frequency(term_dict):
            # FIXME: add filter by term length
            filtered = []
            for k, v in term_dict.items():
                if len(k) > 3:
                    if v > 3:
                        try:
                            float(k)
                            continue
                        except ValueError:
                            matches = re.findall('[0-9]', k)
                            if len(matches) == 0:
                                filtered.append(k)
            return filtered

        text_signatures = []
        total = 0
        for nid in network.iterate_ids_text():
            total += 1
            if total % 100 == 0:
                print("text_sig: " + str(total))
            # We retrieve all documents indexed with the same id in 'text'
            docs = self.get_all_docs_from_text_with_idx_id(nid)
            ids = [x for x in docs]
            # partition ids so that they fit in one http request
            ids_partitions = partition_ids(ids)
            all_terms = defaultdict(int)
            for partition in ids_partitions:
                # We get the term vectors for each group of those documents
                # , body=term_body)
                ans = client.mtermvectors(
                    index='text', ids=partition, doc_type='column')
                # We merge them somehow
                found_docs = ans['docs']
                for doc in found_docs:
                    term_vectors = doc['term_vectors']
                    if 'text' in term_vectors:
                        # terms = list(term_vectors['text']['terms'].keys())
                        terms_and_freq = term_vectors['text']['terms']
                        for term, freq_dict in terms_and_freq.items():
                            # we don't care about the value
                            all_terms[term] = all_terms[
                                                  term] + freq_dict['term_freq']
            filtered_term_vector = filter_term_vector_by_frequency(all_terms)
            if len(filtered_term_vector) > 0:
                data = (nid, filtered_term_vector)
                text_signatures.append(data)
        return text_signatures

    def get_all_mh_text_signatures(self):
        """
        Retrieves id-mh fields
        :return: (fields, numsignatures)
        """
        query_body = {
            "query": {"bool": {"filter": [{"term": {"dataType": "T"}}]}}}
        res = client.search(index='profile', body=query_body, scroll="10m",
                            filter_path=['_scroll_id',
                                         'hits.hits._id',
                                         'hits.total',
                                         'hits.hits._source.minhash']
                            )
        scroll_id = res['_scroll_id']
        remaining = res['hits']['total']

        id_sig = []
        while remaining > 0:
            hits = res['hits']['hits']
            for h in hits:
                data = (h['_id'], h['_source']['minhash'])
                id_sig.append(data)
                remaining -= 1
            res = client.scroll(scroll="5m", scroll_id=scroll_id,
                                filter_path=['_scroll_id',
                                             'hits.hits._id',
                                             'hits.hits._source.minhash']
                                )
            scroll_id = res['_scroll_id']  # update the scroll_id
        client.clear_scroll(scroll_id=scroll_id)
        return id_sig

    def get_all_fields_num_signatures(self):
        """
        Retrieves numerical fields and signatures from the store
        :return: (fields, numsignatures)
        """
        query_body = {
            "query": {"bool": {"filter": [{"term": {"dataType": "N"}}]}}}
        res = client.search(index='profile', body=query_body, scroll="10m",
                            filter_path=['_scroll_id',
                                         'hits.hits._id',
                                         'hits.total',
                                         'hits.hits._source.median',
                                         'hits.hits._source.iqr',
                                         'hits.hits._source.minValue',
                                         'hits.hits._source.maxValue']
                            )
        scroll_id = res['_scroll_id']
        remaining = res['hits']['total']

        id_sig = []
        while remaining > 0:
            hits = res['hits']['hits']
            for h in hits:
                data = (h['_id'], (h['_source']['median'], h['_source']['iqr'],
                                   h['_source']['minValue'], h['_source']['maxValue']))
                id_sig.append(data)
                remaining -= 1
            res = client.scroll(scroll="5m", scroll_id=scroll_id,
                                filter_path=['_scroll_id',
                                             'hits.hits._id',
                                             'hits.hits._source.median',
                                             'hits.hits._source.iqr',
                                             'hits.hits._source.minValue',
                                             'hits.hits._source.maxValue']
                                )
            scroll_id = res['_scroll_id']  # update the scroll_id
        client.clear_scroll(scroll_id=scroll_id)
        return id_sig

    def delete_all_metadata(self):
      """
      Deletes all documents indexed of index='metadata' and doc_type='annotation'.
      For testing purposes.
      """
      res = self.get_all_metadata_fields()["hits"]["hits"]
      for annotation in res:
        client.delete(index='metadata', doc_type='annotation', id=annotation["_id"])
      return self.get_all_metadata_fields()

    def write_metadata(self, author: str, md_class: str):
      """
      :param author: user or process who wrote the metadata
      :param class: warning, insight, or question
      :param source: nid of column source
      :param target: nid of column 
      :param relation: 

      """
      body = {
        "author": author,
        "class": md_class,
        "source": "SOURCE",
        "ref": {
          "target": "TARGET",
          "type": "REF_TYPE"
        }
      }
      res = client.create(index='metadata', doc_type='annotation', body=body)
      return res

    def get_all_metadata_fields(self):
      """
      Returns all metadata fields.
      """
      body = {"query": {"match_all": {}}}
      res = client.search(index='metadata', body=body, scroll="10m")
      return res

    def get_all_metadata_fields_with(self, node):
      """
      Returns all metadata fields that reference the given nid or node.
      """
      pass


if __name__ == "__main__":
    print("Elastic Store")

    """
        def get_all_text_fields(self):
            query_body = {
                "query": {"bool": {"filter": [{"term": {"dataType": "T"}}]}}}
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

        def get_fields_text_index(self):
            '''
            Reads all fields, described as (id, source_name, field_name) from the store (text index).
            :return: a list of all fields with the form (id, source_name, field_name)
            '''
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
                    hit = Hit(h['_id'], h['_source']['sourceName'],
                              h['_source']['columnName'], -1)
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
        """
