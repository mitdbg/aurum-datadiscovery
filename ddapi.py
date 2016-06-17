from modelstore.elasticstore import StoreHandler
from modelstore.elasticstore import KWType
from knowledgerepr.fieldnetwork import Relation
from knowledgerepr import fieldnetwork

from collections import defaultdict

store_client = None


class DDAPI:

    __network = None

    def __init__(self, network):
        self.__network = network

    """
    Primitive API
    """

    def kw_search(self, kw):
        hits = store_client.search_keywords(kw, KWType.KW_TEXT)
        return hits

    def schema_search(self, kw):
        hits = store_client.search_keywords(kw, KWType.KW_SCHEMA)
        return hits

    def entity_search(self, kw):
        hits = store_client.search_keywords(kw, KWType.KW_ENTITIES)
        return hits

    def schema_neighbors(self, field):
        hits = self.__network.neighbors(field, Relation.SCHEMA)
        return hits

    def schema_sim_fields(self, field):
        hits = self.__network.neighbors(field, Relation.SCHEMA_SIM)
        return hits

    def similar_content_fields(self, field):
        hits = self.__network.neighbors(field, Relation.CONTENT_SIM)
        return hits

    def similar_entities_fields(self, field):
        hits = self.__network.neighbors(field, Relation.ENTITY_SIM)
        return hits

    def overlap_fields(self, field):
        hits = self.__network.neighbors(field, Relation.OVERLAP)
        return hits

    def pkfk_fields(self, field):
        hits = self.__network.neighbors(field, Relation.PKFK)
        return hits

    """
    Combiner API
    """

    def and_conjunctive(self, a, b):
        sa = set(a)
        sb = set(b)
        res = sa.intersection(sb)
        return res

    def or_conjunctive(self, a, b):
        res = set(a).union(set(b))
        return res

    def has_path(self, source, target, relation):
        path = self.__network.find_path(source, target, relation)
        return path

    def in_context_with(self, a, b, relation):
        res = set()
        for el1 in a:
            for el2 in b:
                path = self.__network.find_path(el1, el2, relation)
                if el1 in path:
                    res.add(el1)
        return res


    """
    Function API
    """

    def join_path(self, source, target):
        first_class_path = self.__network.find_path(source, target, Relation.PKFK)
        second_class_path = self.__network.find_path(source, target, Relation.CONTENT_SIM)
        path = ([].extend(first_class_path)).extend(second_class_path)
        return path

    def schema_complement(self, source_name):
        """
        Given a table of reference (table_to_enrich) it uses information from
        other available tables (tables) to enrich the schema -- to add columns
        """
        # Retrieve all fields of the given source
        results = store_client.get_all_fields_of_source(source_name)
        res = [x for x in results]
        matches = list()
        # Find if there are pkfk connections for any of the columns
        for r in res:
            id, source_name, field_name = r
            q = (source_name, field_name)
            ns = self.__network.neighbors(q, Relation.PKFK)
            for neighbor in ns:
                matches.append(neighbor)

        # Find if there are any content_sim connection
        for r in res:
            id, source_name, field_name = r
            q = (source_name, field_name)
            ns = self.__network.neighbors(q, Relation.SCHEMA_SIM)
            for neighbor in ns:
                matches.append(neighbor)

        return res

    def entity_complement(self):
        """
        Given a table of reference (table_to_enrich) it uses information from
        other available tables (tables) to add entities -- to add rows
        """
        print("TODO")

    def fill_schema(self, virtual_schema):
        tokens = virtual_schema.split(",")
        tokens = [t.strip() for t in tokens]
        aprox = dict()
        # Find set of schema_sim for each field provided in the virtual schema
        for t in tokens:
            hits = self.schema_search(t)
            aprox[t] = hits

        # Find the most suitable table, by checking which of the previous fields are schema-connected,
        # and selecting the set with the biggest size.
        # TODO:

        # Use table as seed. Find PKFK for any of the fields in the table, that may join to the other
        # requested attributes
        # TODO:

        return aprox

    def find_tables_matching_schema(self, list_keywords, topk):
        """
        def _attr_similar_to(keyword, topk, score):
            # TODO: handle multiple input keywords
            similarity_map = dict()
            kw = keyword.lower()
            for (fname, cname) in concepts:
                p = cname.lower()
                p_tokens = p.split(' ')
                for tok in p_tokens:
                    # compute similarity and put in dict if beyond a
                    distance = editdistance.eval(kw, tok)
                    # minimum threshold
                    if distance < C.max_distance_schema_similarity:
                        similarity_map[(fname, cname)] = distance
                        break  # to avoid potential repetitions
            sorted_sim_map = sorted(similarity_map.items(), key=operator.itemgetter(1))
            if score:
                return sorted_sim_map[:topk]
            else:
                noscore_res = [n for (n, score) in sorted_sim_map[:topk]]
                return noscore_res
        """
        def attr_similar_to(keyword, topk, score):
            results = self.schema_search(keyword)
            r = [(x.source_name, x.field_name, x.score) for x in results]
            return r[:topk]

        '''
        Return list of tables that contain the required schema
        '''
        all_res = []
        # First get results for each of the provided keywords
        # (input_keyword , [((table, column), score)]
        for keyword in list_keywords:
            res = attr_similar_to(keyword, 30, False)
            all_res.append((keyword, res))

        # Group by tables, and include the (kw, score) that matched
        group_by_table_keyword = dict()
        for (keyword, res) in all_res:
            for (fname, cname, score) in res:
                if fname not in group_by_table_keyword:
                    group_by_table_keyword[fname] = []
                included = False
                for kw, score in group_by_table_keyword[fname]:
                    if keyword.lower() == kw.lower():
                        included = True  # don't include more than once
                if not included:
                    group_by_table_keyword[fname].append((keyword, score))
                else:
                    continue

        # Create final output
        to_return = sorted(group_by_table_keyword.items(), key=lambda x: len(x[1]), reverse=True)
        return to_return[:topk]


class ResultFormatter:

    @staticmethod
    def format_output_for_webclient(raw_output, consider_col_sel):
        """
        Format raw output into something client understands,
        mostly, enrich the data with schema and samples
        """

        def get_repr_columns(source_name, columns, consider_col_sel):
            def set_selected(c):
                if consider_col_sel:
                    if c in columns:
                        return 'Y'
                return 'N'
            # Get all fields in source_name
            all_fields = store_client.get_all_fields_of_source(source_name)
            colsrepr = []
            for (nid, sn, fn) in all_fields:
                colrepr = {
                    'colname': fn,
                    'samples': store_client.peek_values((sn, fn), 15),  # ['fake1', 'fake2'], p.peek((fname, c), 15),
                    'selected': set_selected(fn)
                }
                colsrepr.append(colrepr)
            return colsrepr

        entries = []
        # Group results into a dict with file -> [column]
        group_by_file = dict()
        for (fname, cname) in raw_output:
            if fname not in group_by_file:
                group_by_file[fname] = []
            group_by_file[fname].append(cname)
        # Create entry per filename
        for fname, columns in group_by_file.items():
            entry = {'filename': fname,
                     'schema': get_repr_columns(
                         fname,
                         columns,
                         consider_col_sel)
                     }
            entries.append(entry)
        return entries

    @staticmethod
    def format_output_for_webclient_ss(raw_output, consider_col_sel):
        """
        Format raw output into something client understands.
        The output in this case is the result of a table search.
        """
        def get_repr_columns(source_name, columns, consider_col_sel):
            def set_selected(c):
                if consider_col_sel:
                    if c in columns:
                        return 'Y'
                return 'N'
            # Get all fields of source_name
            all_fields = store_client.get_all_fields_of_source(source_name)
            all_cols = [fn for (nid, sn, fn) in all_fields]
            for myc in columns:
                all_cols.append(myc)
            colsrepr = []
            for c in all_cols:
                colrepr = {
                    'colname': c,
                    'samples': store_client.peek_values((source_name, c), 15),
                    'selected': set_selected(c)
                }
                colsrepr.append(colrepr)
            return colsrepr

        entries = []

        # Create entry per filename
        # for fname, columns in group_by_file.items():
        for fname, column_scores in raw_output:
            columns = [c for (c, _) in column_scores]
            entry = {'filename': fname,
                     'schema': get_repr_columns(
                         fname,
                         columns,
                         consider_col_sel)
                     }
            entries.append(entry)
        return entries


class API(DDAPI):

    def __init__(self, *args, **kwargs):
        super(API, self).__init__(*args, **kwargs)

    def init_store(self):
        # create store handler
        global store_client
        store_client = StoreHandler()

if __name__ == '__main__':

    # create store handler
    store_client = StoreHandler()
    # read graph
    path = 'test/network.pickle'
    network = fieldnetwork.deserialize_network(path)
    api = API(network)

    #####
    # testing index primitives
    #####

    print("Keyword search in text")
    results = api.kw_search("Michael")
    for r in results:
        print(str(r))

    print("Keyword search in schema names")
    results = api.schema_search("MIT")
    for r in results:
        print(str(r))

    print("Keyword search in entities")
    results = api.entity_search('person')
    for r in results:
        print(str(r))

    #####
    # testing graph primitives
    #####

    field = ('Iap_subject_person.csv', 'Person Mit Affiliation')
    print("")
    print("Relations of: " + str(field))

    print("")
    print("Schema conn")
    print("")
    nodes = api.schema_neighbors(field)
    for node in nodes:
        print(node)

    print("")
    print("Schema SIM")
    print("")
    nodes = api.schema_sim_fields(field)
    for node in nodes:
        print(node)

    print("")
    print("Content sim")
    print("")
    nodes = api.similar_content_fields(field)
    for node in nodes:
        print(node)

    print("")
    print("Entity sim")
    print("")
    nodes = api.similar_entities_fields(field)
    for node in nodes:
        print(node)

    print("")
    print("Overlap")
    print("")
    nodes = api.overlap_fields(field)
    for node in nodes:
        print(node)

    print("")
    print("PKFK")
    print("")
    nodes = api.pkfk_fields(field)
    for node in nodes:
        print(node)

    ######
    # Combiner functions
    ######

    print("Combiner AND")
    results1 = api.kw_search("Michael")
    results2 = api.kw_search("Barbara")
    final = api.and_conjunctive(results1, results2)

    print(str(len(final)))

    for el in final:
        print(str(el))

    print("Combiner OR")
    results1 = api.kw_search("Michael")
    results2 = api.kw_search("Barbara")
    final = api.or_conjunctive(results1, results2)

    print(str(len(final)))

    for el in final:
        print(str(el))

    print("Context combiner")
    results1 = api.kw_search("Michael")
    results2 = api.kw_search("Barbara")
    path = api.in_context_with(results1, results2, Relation.SCHEMA)
    for el in path:
        print(str(el))

    print("Function API")
    print("Add column")
    sn = "Hr_faculty_roster.csv"
    list_of_results = api.schema_complement(sn)
    for l in list_of_results:
        print(str(l))

    list_of_results = api.fill_schema("First name, department, schedule")
    for k,v in list_of_results.items():
        print("###")
        print("")
        print(str(k))
        for value in v:
            print(str(value))
