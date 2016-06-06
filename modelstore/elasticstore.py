from elasticsearch import Elasticsearch
import config as c


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
                                         'hits.hits._source.sourceName',
                                         'hits.hits._source.columnName']
                            )
        scroll_id = res['_scroll_id']
        remaining = res['hits']['total']
        while remaining > 0:
            hits = res['hits']['hits']
            for h in hits:
                id_source_and_file_name = (h['_id'], h['_source']['sourceName'], h['_source']['columnName'])
                yield id_source_and_file_name
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

    def peek_values(self, field, num_values):
        """
        Reads sample values for the given field
        :param field: The field from which to read values
        :param num_values: The number of values to read
        :return: A list with the sample values read for field
        """
        print("TODO")

    def search_keywords(self, keywords, elasticfieldname):
        """
        Performs a search query on elastic_field_name to match the provided keywords
        :param keywords: the list of keyword to match
        :return: the list of documents that contain the keywords
        """
        print("TODO")

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

    def get_all_fields_textsignatures(self):
        """
        Retrieves textual fields and signatures from the store
        :return: (fields, textsignatures)
        """
        # TODO: do this only for textual columns
        term_body = {"filter": {"min_term_freq": 2, "max_num_terms": 25}}

        fields = []
        seen_nid = []
        text_signatures = []
        text_fields_gen = self.get_fields_text_index()
        for (rawid, nid, sn, fn) in text_fields_gen:
            if nid not in seen_nid:
                fields.append((nid, sn, fn))
                ans = client.termvectors(index='text', id=rawid, doc_type='column', body=term_body)
                terms = []
                if ans['found']:
                    term_vectors = ans['term_vectors']
                    if 'text' in term_vectors:
                        terms = list(ans['term_vectors']['text']['terms'].keys())
                text_signatures.append(terms)
            seen_nid.append(nid)
        return (fields, text_signatures)

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
    # all_fields = handler.get_all_fields_with(['maxValue', 'minValue'])
    # i = 0
    # for el in all_fields:
    #    print(str(el))
    # print("Total fields: " + str(i))

    #data = handler.get_all_fields_textsignatures()
    #fields, text_sig = data
    #for sig in text_sig:
    #    print(str(sig))

    data = handler.get_all_fields_numsignatures()
    fields, num_sig = data
    for sig in num_sig:
        print(str(sig))
