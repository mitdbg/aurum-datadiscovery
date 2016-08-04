from modelstore.elasticstore import StoreHandler
from modelstore.elasticstore import KWType
from knowledgerepr.fieldnetwork import Relation
from knowledgerepr.fieldnetwork import Hit
from knowledgerepr.fieldnetwork import compute_field_id as id_from
from knowledgerepr import fieldnetwork
from api.apiutils import DRS
from api.apiutils import DRSMode

store_client = None


class DDAPI:

    __network = None

    def __init__(self, network):
        self.__network = network

    """
    Seed API
    """
    def drs_from(self, field: str, source: str) -> DRS:
        """
        Given a field and source name, it returns a DRS with its representation
        :param field: a string with the name of the field
        :param source: a string with the name of the source
        :return: a DRS with the source-field internal representation
        """
        nid = id_from(source, field)
        h = Hit(nid, source, field, -1)
        drs = DRS([h])
        return drs

    def drs_from_table(self, source: str) -> DRS:
        """
        Given a source, it retrieves all fields of the source and returns them
        in the internal representation
        :param source: string with the name of the table
        :return: a DRS with the source-field internal representation
        """
        hits = store_client.get_all_fields_of_source(source)
        drs = DRS([x for x in hits])
        return drs

    """
    View API
    """

    def fields(self, drs: DRS) -> DRS:
        """
        Given a DRS, it configures it to field view (default)
        :param drs: the DRS to configure
        :return: the same DRS in the fields mode
        """
        drs.set_fields_mode()
        return drs

    def table(self, drs: DRS) -> DRS:
        """
        Given a DRS, it configures it to the table view
        :param drs: the DRS to configure
        :return: the same DRS in the table mode
        """
        drs.set_table_mode()
        return drs

    """
    Primitive API
    """

    def keyword_search(self, kw: str, max_results=10) -> DRS:
        """
        Performs a keyword search over the content of the data
        :param kw: the keyword to search
        :param max_results: the maximum number of results to return
        :return: returns a DRS
        """
        hits = store_client.search_keywords(kw, KWType.KW_TEXT, max_results)
        drs = DRS([x for x in hits])  # materialize generator
        return drs

    def keywords_search(self, kws: [str]) -> DRS:
        """
        Given a collection of keywords, it returns the matches in the internal representation
        :param kws: collection (iterable) of keywords (strings)
        :return: the matches in the internal representation
        """
        res = DRS([])
        for kw in kws:
            drs = self.keyword_search(kw)
            self.union(res, drs)
        return res

    def schema_name_search(self, kw: str, max_results=10) -> DRS:
        """
        Performs a keyword search over the attribute/field names of the data
        :param kw: the keyword to search
        :param max_results: the maximum number of results to return
        :return: returns a DRS
        """
        hits = store_client.search_keywords(kw, KWType.KW_SCHEMA, max_results)
        drs = DRS([x for x in hits])  # materialize generator
        return drs

    def schema_names_search(self, kws: [str]) -> DRS:
        """
        Given a collection of schema names, it returns the matches in the internal representation
        :param kws: collection (iterable) of keywords (strings)
        :return: a DRS
        """
        res = DRS([])
        for kw in kws:
            drs = self.schema_name_search(kw)
            self.union(res, drs)
        return res

    def entity_search(self, kw: str, max_results=10) -> DRS:
        """
        Performs a keyword search over the entities represented by the data
        :param kw: the keyword to search
        :param max_results: the maximum number of results to return
        :return: returns a list of Hit elements of the form (id, source_name, field_name, score)
        """
        hits = store_client.search_keywords(kw, KWType.KW_ENTITIES, max_results)
        drs = DRS([x for x in hits])  # materialize generator
        return drs

    def schema_neighbors(self, field: str) -> DRS:
        """
        Returns all the other attributes/fields that appear in the same relation than the provided field
        :param field: the provided field
        :return: returns a list of Hit elements of the form (id, source_name, field_name, score)
        """
        hits = self.__network.neighbors(field, Relation.SCHEMA)
        drs = DRS([x for x in hits])
        return drs

    def similar_schema_name_to_field(self, field: str) -> DRS:
        """
        Returns all the attributes/fields with schema names similar to the provided field
        :param field: the provided field
        :return: returns a list of Hit elements of the form (id, source_name, field_name, score)
        """
        hits = self.__network.neighbors(field, Relation.SCHEMA_SIM)
        drs = DRS([x for x in hits])
        return drs

    def similar_schema_name_to_table(self, table: str) -> DRS:
        """
        Returns all the attributes/fields with schema names similar to the fields of the given table
        :param table: the given table
        :return: DRS
        """
        res = DRS([])
        fields = self.fields_of_table(table)
        for f in fields:
            drs = self.similar_schema_name_to_field(f.field_name)
            self.union(res, drs)
        return res

    def similar_schema_name_to(self, i_drs: DRS) -> DRS:
        """
        Given a DRS it returns another DRS that contains all fields similar to the fields of the input
        :param i_drs: the input DRS
        :return: DRS
        """
        o_drs = DRS([])
        if i_drs.mode == DRSMode.FIELDS:
            for h in i_drs:
                hits = self.__network.neighbors_id(h.nid, Relation.SCHEMA_SIM)
                res_drs = DRS([x for x in hits])
                o_drs = self.union(o_drs, res_drs)
        elif i_drs.mode == DRSMode.TABLE:
            for table in i_drs:
                fields_drs = self.drs_from_table(table)
                for h in fields_drs:
                    hits = self.__network.neighbors_id(h.nid, Relation.SCHEMA_SIM)
                    res_drs = DRS([x for x in hits])
                    o_drs = self.union(o_drs, res_drs)
        return o_drs

    def similar_content_to_field(self, field: str) -> DRS:
        """
        Returns all the attributes/fields with content similar to the provided field
        :param field: the provided field
        :return: returns a list of Hit elements of the form (id, source_name, field_name, score)
        """
        hits = self.__network.neighbors(field, Relation.CONTENT_SIM)
        drs = DRS([x for x in hits])
        return drs

    def similar_content_to_table(self, table: str) -> DRS:
        res = DRS([])
        fields = self.fields_of_table(table)
        for f in fields:
            drs = self.similar_content_to_field(f.field_name)
            self.union(res, drs)
        return res

    def similar_content_to(self, i_drs: DRS) -> DRS:
        """
        Given a DRS it returns another DRS that contains all fields similar to the fields of the input
        :param i_drs: the input DRS
        :return: DRS
        """
        o_drs = DRS([])
        if i_drs.mode == DRSMode.FIELDS:
            for h in i_drs:
                hits = self.__network.neighbors_id(h.nid, Relation.CONTENT_SIM)
                res_drs = DRS([x for x in hits])
                o_drs = self.union(o_drs, res_drs)
        elif i_drs.mode == DRSMode.TABLE:
            for table in i_drs:
                fields_drs = self.drs_from_table(table)
                for h in fields_drs:
                    hits = self.__network.neighbors_id(h.nid, Relation.CONTENT_SIM)
                    res_drs = DRS([x for x in hits])
                    o_drs = self.union(o_drs, res_drs)
        return o_drs

    def similar_entity_to_field(self, field: str) -> DRS:
        """
        Returns all the attributes/fields that represent entities similar to the provided field
        :param field: the provided field
        :return: returns a list of Hit elements of the form (id, source_name, field_name, score)
        """
        hits = self.__network.neighbors(field, Relation.ENTITY_SIM)
        drs = DRS([x for x in hits])
        return drs

    def pkfk_field(self, field: str) -> DRS:
        """
        Returns all the attributes/fields that are primary-key or foreign-key candidates with respect to the
        provided field
        :param field: the providef field
        :return: returns a list of Hit elements of the form (id, source_name, field_name, score)
        """
        hits = self.__network.neighbors(field, Relation.PKFK)
        drs = DRS([x for x in hits])
        return drs

    def pkfk_table(self, table: str) -> DRS:
        res = DRS([])
        fields = self.fields_of_table(table)
        for f in fields:
            drs = self.pkfk_field(f.field_name)
            self.union(res, drs)
        return res

    def pkfk_of(self, i_drs: DRS) -> DRS:
        """
        Given a DRS it returns another DRS that contains all fields similar to the fields of the input
        :param i_drs: the input DRS
        :return: DRS
        """
        o_drs = DRS([])
        if i_drs.mode == DRSMode.FIELDS:
            for h in i_drs:
                hits = self.__network.neighbors_id(h.nid, Relation.PKFK)
                res_drs = DRS([x for x in hits])
                o_drs = self.union(o_drs, res_drs)
        elif i_drs.mode == DRSMode.TABLE:
            for table in i_drs:
                fields_drs = self.drs_from_table(table)
                for h in fields_drs:
                    hits = self.__network.neighbors_id(h.nid, Relation.PKFK)
                    res_drs = DRS([x for x in hits])
                    o_drs = self.union(o_drs, res_drs)
        return o_drs

    """
    Combiner API
    """

    def intersection(self, a: DRS, b: DRS) -> DRS:
        """
        Returns elements that are both in a and b
        :param a: an iterable object
        :param b: another iterable object
        :return: the intersection of the two provided iterable objects
        """
        assert(a.mode == b.mode)
        sa = set(a.data)
        sb = set(b.data)
        res = sa.intersection(sb)
        return DRS(list(res))

    def union(self, a: DRS, b: DRS) -> DRS:
        """
        Returns elements that are in either a or b
        :param a: an iterable object
        :param b: another iterable object
        :return: the union of the two provided iterable objects
        """
        assert (a.mode == b.mode)
        res = set(a.data).union(set(b.data))
        return DRS(list(res))

    def difference(self, a: DRS, b: DRS) -> DRS:
        """
        Returns elements that are in either a or b
        :param a: an iterable object
        :param b: another iterable object
        :return: the union of the two provided iterable objects
        """
        assert (a.mode == b.mode)
        res = set(a.data) - set(b.data)
        return DRS(list(res))

    """
    TC Primitive API
    """

    def get_path(self, source, target, relation):
        """
        Returns the path that connects source and target, if any
        :param source: the source field
        :param target: the target field
        :param relation: the type of relation that may/may not connect source and target
        :return: a path if exists
        """
        path = self.__network.find_path(source, target, relation)
        return path

    def paths_field(self, a: str, b: str, primitives) -> DRS:
        return

    def paths_table(self, a: str, b: str, primitives) -> DRS:
        return

    def paths(self, a: DRS, b: DRS, primitives) -> DRS:
        o_drs = DRS([])
        for h1 in a:
            for h2 in b:
                res_drs = self.__network.find_path_hit(h1, h2, primitives)
                o_drs = self.union(o_drs, res_drs)
        return o_drs

    def paths_fields(self, a, primitives) -> DRS:
        return

    def paths(self, a: DRS) -> DRS:
        return

    def traverse_field(self, a, primitives, max_hops) -> DRS:
        return

    def in_context_with(self, a, b, relation):
        """
        Returns the nodes from a that are connected to any node in b
        :param a: an iterable object
        :param b: another iterable object
        :param relation: the type of relation that may/may not connect nodes in a with nodes in b
        :return: a set of results from a that are connected through relation to nodes in b
        """
        res = set()
        for el1 in a:
            for el2 in b:
                path = self.get_path(el1, el2, relation)
                if el1 in path:
                    res.add(el1)
        return res

    """
    Convenience functions
    """

    def output_raw(self, result_set):
        """
        Given an iterable object it prints the raw elements
        :param result_set: an iterable object
        """
        for r in result_set:
            print(str(r))

    def output(self, result_set):
        """
        Given an iterable object of elements of the form (nid, source_name, field_name, score) it prints
        the source and field names for every element in the iterable
        :param result_set: an iterable object
        """
        for r in result_set:
            (nid, sn, fn, s) = r
            print("source: " + str(sn) + "\t\t\t\t\t field: " + fn)

    def help(self):
        """
        Prints general help information, or specific usage information of a function if provided
        :param function: an optional function
        """
        from IPython.display import Markdown, display

        def print_md(string):
            display(Markdown(string))

        # Check whether the request is for some specific function
        #if function is not None:
        #    print_md(self.function.__doc__)
        # If not then offer the general help menu
        #else:
        print_md("### Help Menu")
        print_md("You can use the system through an **API** object. API objects are returned"
                 "by the *init_system* function, so you can get one by doing:")
        print_md("***your_api_object = init_system('path_to_stored_model')***")
        print_md("Once you have access to an API object there are a few concepts that are useful "
                 "to use the API. **content** refers to actual values of a given field. For "
                 "example, if you have a table with an attribute called __Name__ and values *Olu, Mike, Sam*, content "
                 "refers to the actual values, e.g. Mike, Sam, Olu.")
        print_md("**schema** refers to the name of a given field. In the previous example, schema refers to the word"
                 "__Name__ as that's how the field is called.")
        print_md("Finally, **entity** refers to the *semantic type* of the content. This is in experimental state. For "
                 "the previous example it would return *'person'* as that's what those names refer to.")
        print_md("Certain functions require a *field* as input. In general a field is specified by the source name ("
                 "e.g. table name) and the field name (e.g. attribute name). For example, if we are interested in "
                 "finding content similar to the one of the attribute *year* in the table *Employee* we can provide "
                 "the field in the following way:")
        print("field = ('Employee', 'year') # field = [<source_name>, <field_name>)")

    """
    Function API
    """

    def join_path(self, source, target):
        """
        Provides the join path between the source and target fields, if any
        :param source: the source field
        :param target: the target field
        :return: the join path between source and target if any
        """
        first_class_path = self.__network.find_path(source, target, Relation.PKFK)
        second_class_path = self.__network.find_path(source, target, Relation.CONTENT_SIM)
        path = first_class_path.extend(second_class_path)
        if path is None:
            return []
        return path

    def schema_complement(self, source_name):
        """
        Given a source of reference (e.g. a relational table) it uses information from
        other available tables (tables) to enrich the schema of the provided one -- to add columns
        :param source_name: the name of the reference source
        :return:
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
            ns = self.__network.neighbors(q, Relation.CONTENT_SIM)
            for neighbor in ns:
                matches.append(neighbor)

        return matches

    def __entity_complement(self):
        """
        Given a table of reference (table_to_enrich) it uses information from
        other available tables (tables) to add entities -- to add rows
        """
        print("TODO")

    def __fill_schema(self, virtual_schema):
        tokens = virtual_schema.split(",")
        tokens = [t.strip() for t in tokens]
        aprox = dict()
        # Find set of schema_sim for each field provided in the virtual schema
        for t in tokens:
            hits = self.schema_name_search(t)
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
        Given a string with comma separated values, such as 'x, y, z', it tries to find
        tables in the data that contains as many of the attributes included in the original string,
        for the given example, tables with attributes x and y and z
        :param list_keywords: the string with comma separated keywords
        :param topk: the maximum number of results to return
        :return: topk results or as many as available
        """

        def attr_similar_to(keyword, topk, score):
            results = self.schema_name_search(keyword, max_results=100)
            r = [(x.source_name, x.field_name, x.score) for x in results]
            return r

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

def test_all():
    # create store handler
    store_client = StoreHandler()
    # read graph
    path = 'test/network.pickle'
    network = fieldnetwork.deserialize_network(path)
    api = API(network)
    api.init_store()

    #####
    # testing index primitives
    #####

    print("Keyword search in text")
    results = api.keyword_search("Michael")
    for r in results:
        print(str(r))

    print("Keyword search in schema names")
    results = api.schema_name_search("MIT")
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
    nodes = api.similar_schema_name_to_field(field)
    for node in nodes:
        print(node)

    print("")
    print("Content sim")
    print("")
    nodes = api.similar_content_to_field(field)
    for node in nodes:
        print(node)

    print("")
    print("Entity sim")
    print("")
    nodes = api.similar_entity_to_field(field)
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
    nodes = api.pkfk_field(field)
    for node in nodes:
        print(node)

    ######
    # Combiner functions
    ######

    print("Combiner AND")
    results1 = api.keyword_search("Michael")
    results2 = api.keyword_search("Barbara")
    final = api.intersection(results1, results2)

    print(str(len(final)))

    for el in final:
        print(str(el))

    print("Combiner OR")
    results1 = api.keyword_search("Michael")
    results2 = api.keyword_search("Barbara")
    final = api.union(results1, results2)

    print(str(len(final)))

    for el in final:
        print(str(el))

    print("Context combiner")
    results1 = api.keyword_search("Michael")
    results2 = api.keyword_search("Barbara")
    path = api.in_context_with(results1, results2, Relation.SCHEMA)
    for el in path:
        print(str(el))

    #########
    # Discovery functions
    #########

    print("Function API")

    print("Add column")
    sn = "Hr_faculty_roster.csv"
    list_of_results = api.schema_complement(sn)
    for l in list_of_results:
        print(str(l))

    print("Fill Schema")
    list_of_results = api.fill_schema("First name, department, schedule")
    for k, v in list_of_results.items():
        print("###")
        print("")
        print(str(k))
        for value in v:
            print(str(value))

def test():
    # create store handler
    store_client = StoreHandler()
    # read graph
    path = 'test/network.pickle'
    network = fieldnetwork.deserialize_network(path)
    api = API(network)

    field1 = ('short_subjects_offered.csv', 'Responsible Faculty Name')
    field2 = ('short_subjects_offered.csv', 'Responsible Faculty Mit Id')
    field3 = ('short_subjects_offered.csv', 'Offer Dept Name')
    field4 = ('Sis_department.csv', 'Dept Name In Commencement Bk')
    field5 = ('short_cis_course_catalog.csv', 'Department Name')
    field6 = ('Fac_building.csv', 'Building Name Long')
    field7 = ('Fac_building.csv', 'Site')

    similar_set = api.similar_content_to_field(field7)
    ss = [x for x in similar_set]
    print(str(len(ss)))
    for el in ss:
        print(str(el))


    ### test vectors from signatures with 25 term

    v1 = ['asada', 'haruhiko', 'mark', 'cynthia', 'dow', 'dutta', 'watenpaugh', 'stewart', 'david', 'caso', 'krzysztof', 'haywood', 'qingyan', 'kausel', 'troxel', 'tarkowski', 'hadjiconstantin', 'rene', 'arindam', 'jame', 'gibbon', 'eduardo', 'joe', 'wodiczko', 'john']
    v2 = ['carmichael', 'peter', 'jackson', 'rene', 'fitzgerald', 'cynthia', 'mohr', 'thoma', 'stewart', 'michael', 'david', 'caso', 'jame', 'georg', 'sue', 'sonenberg', 'ann', 'harri', 'mark', 'peterson', 'jean', 'murrai', 'daniel', 'john', 'zhiyuan']
    v3 = ['jane', 'jaim', 'totten', 'paradi', 'yuehua', 'margeri', 'ellen', 'bishwapriya', 'stewart', 'jame', 'peirson', 'charl', 'sabin', 'dan', 'crocker', 'monika', 'perair', 'resnick', 'dunphi', 'levet', 'sanyal', 'samuel', 'jenkin', 'burn', 'fintel']
    v4 = ['kemp', 'peter', 'sadock', 'ayumi', 'lissett', 'fravel', 'nagatomi', 'ellen', 'kardar', 'tegmark', 'michael', 'david', 'jame', 'ann', 'crocker', 'goethert', 'stephen', 'friedman', 'mehrotra', 'robert', 'lee', 'geraldin', 'daniel', 'john', 'grimm']
    v5 = ['sewel', 'peter', 'roylanc', 'smith', 'tonegawa', 'donald', 'alan', 'sawin', 'michael', 'david', 'caso', 'hein', 'georg', 'peirson', 'kausel', 'alvin', 'temin', 'arthur', 'hobb', 'harri', 'scott', 'nigel', 'samuel', 'linn', 'susumu']
    v6 = ['belcher', 'schulz', 'ochsendorf', 'gruber', 'yue', 'schuh', 'chung', 'van', 'singer', 'leiserson', 'white', 'verghes', 'wang', 'dedon', 'sussman', 'grossman', 'miller', 'bowr', 'stephanopoulo', 'lozano', 'paxson', 'sarma', 'sanyal', 'kaiser', 'lee']

    d1 = ' '.join(v1)
    d2 = ' '.join(v2)
    d3 = ' '.join(v3)
    d4 = ' '.join(v4)
    d5 = ' '.join(v5)
    d6 = ' '.join(v6)

    from dataanalysis import dataanalysis as da
    tfidf = da.get_tfidf_docs((d1, d2, d3, d4, d5, d6))
    sparse_r1 = tfidf.getrow(0)
    sparse_r2 = tfidf.getrow(1)
    sparse_r3 = tfidf.getrow(2)
    sparse_r4 = tfidf.getrow(3)
    sparse_r5 = tfidf.getrow(4)
    sparse_r6 = tfidf.getrow(5)

    dense_r1 = sparse_r1.todense()
    dense_r2 = sparse_r2.todense()
    dense_r3 = sparse_r3.todense()
    dense_r4 = sparse_r4.todense()
    dense_r5 = sparse_r5.todense()
    dense_r6 = sparse_r6.todense()

    from scipy import spatial
    cs = 1 - spatial.distance.cosine(dense_r1, dense_r2)
    print("1-2: " + str(cs))
    cs = 1 - spatial.distance.cosine(dense_r1, dense_r3)
    print("1-3: " + str(cs))
    cs = 1 - spatial.distance.cosine(dense_r1, dense_r4)
    print("1-4: " + str(cs))
    cs = 1 - spatial.distance.cosine(dense_r1, dense_r5)
    print("1-5: " + str(cs))
    cs = 1 - spatial.distance.cosine(dense_r1, dense_r6)
    print("1-6: " + str(cs))

    cs = 1 - spatial.distance.cosine(dense_r2, dense_r3)
    print("2-3: " + str(cs))
    cs = 1 - spatial.distance.cosine(dense_r2, dense_r4)
    print("2-4: " + str(cs))
    cs = 1 - spatial.distance.cosine(dense_r2, dense_r5)
    print("2-5: " + str(cs))
    cs = 1 - spatial.distance.cosine(dense_r2, dense_r6)
    print("2-6: " + str(cs))

    cs = 1 - spatial.distance.cosine(dense_r3, dense_r4)
    print("3-4: " + str(cs))
    cs = 1 - spatial.distance.cosine(dense_r3, dense_r5)
    print("3-5: " + str(cs))
    cs = 1 - spatial.distance.cosine(dense_r3, dense_r6)
    print("3-6: " + str(cs))

    cs = 1 - spatial.distance.cosine(dense_r4, dense_r5)
    print("4-5: " + str(cs))
    cs = 1 - spatial.distance.cosine(dense_r4, dense_r6)
    print("4-6: " + str(cs))

    cs = 1 - spatial.distance.cosine(dense_r5, dense_r6)
    print("5-6: " + str(cs))

    ## Check vectors from signatures with 50 term

    v1 = ['ayumi', 'steven', 'nichola', 'dunphi', 'dagmar', 'sadock', 'daniel', 'geraldin', 'chiang', 'jae', 'goethert', 'lee', 'min', 'ben', 'paulin', 'grimm', 'jaeger', 'ann', 'grave', 'david', 'fravel', 'arian', 'soto', 'ian', 'mehrotra', 'spirn', 'robert', 'peter', 'jame', 'john', 'terri', 'tegmark', 'nagatomi', 'maier', 'harri', 'stephen', 'ellen', 'lissett', 'mehran', 'culot', 'michael', 'joseph', 'patricia', 'crocker', 'levet', 'kardar', 'kemp', 'susskind', 'friedman', 'reinhard']
    v2 = ['zemba', 'hashemi', 'frederick', 'fernandez', 'jona', 'garri', 'robert', 'mark', 'saka', 'millon', 'heghnar', 'gibbon', 'harold', 'haruhiko', 'leist', 'caso', 'david', 'william', 'john', 'schrenk', 'kausel', 'mc', 'belang', 'arindam', 'jame', 'stewart', 'chen', 'brisson', 'dutta', 'rene', 'cynthia', 'naginski', 'reiner', 'qingyan', 'dubowski', 'eduardo', 'asada', 'haywood', 'troxel', 'joe', 'tarkowski', 'dow', 'watenpaugh', 'jarzombek', 'christin', 'krzysztof', 'nannaji', 'joan', 'wodiczko', 'hadjiconstantin']
    v3 = ['sanyal', 'amarasingh', 'rene', 'j', 'daniel', 'perair', 'william', 'mark', 'schneider', 'bishwapriya', 'kenneth', 'cui', 'ferreira', 'donald', 'jaim', 'thoma', 'caso', 'david', 'jo', 'sue', 'jackson', 'arthur', 'brown', 'mohr', 'yue', 'robert', 'peter', 'chen', 'stewart', 'john', 'charl', 'ann', 'carmichael', 'peterson', 'sonenberg', 'cynthia', 'henni', 'harri', 'zhiyuan', 'georg', 'jame', 'janet', 'cohen', 'frederick', 'michael', 'paul', 'jean', 'linda', 'fitzgerald', 'murrai']
    v4 = ['wilson', 'cohen', 'linn', 'rene', 'daniel', 'herbert', 'tonegawa', 'sewel', 'hobb', 'schneider', 'scott', 'kenneth', 'peirson', 'alan', 'donald', 'thoma', 'caso', 'david', 'jeffrei', 'john', 'arthur', 'kausel', 'mc', 'smith', 'peter', 'jame', 'chen', 'alvin', 'w', 'joshua', 'harri', 'gregori', 'eduardo', 'georg', 'nigel', 'g', 'temin', 'sawin', 'roylanc', 'michael', 'samuel', 'susan', 'hein', 'susumu', 'ronald', 's', 'roger', 'richard', 'bruce', 'jack']
    v5 = ['henri', 'sanyal', 'robert', 'perair', 'dunphi', 'elizabeth', 'jame', 'iren', 'lawrenc', 'margeri', 'totten', 'levet', 'sabin', 'monika', 'stewart', 'liu', 'ellen', 'fintel', 'charl', 'burn', 'bishwapriya', 'jenkin', 'peter', 'jane', 'michael', 'samuel', 'jaim', 'yuehua', 'von', 'crocker', 'paradi', 'kai', 'richard', 'dan', 'resnick', 'peirson']
    v6 = ['winston', 'verghes', 'yue', 'sanyal', 'miller', 'stephanopoulo', 'doyl', 'singer', 'kaiser', 'chen', 'hu', 'freeman', 'zhang', 'paxson', 'von', 'lee', 'wang', 'schulz', 'ochsendorf', 'gruber', 'perez', 'chung', 'white', 'grossman', 'sussman', 'van', 'smith', 'lozano', 'ting', 'cohen', 'leiserson', 'schuh', 'sarma', 'johnson', 'rose', 'hart', 'bowr', 'ross', 'dedon', 'william', 'thompson', 'belcher', 'zhao']

    d1 = ' '.join(v1)
    d2 = ' '.join(v2)
    d3 = ' '.join(v3)
    d4 = ' '.join(v4)
    d5 = ' '.join(v5)
    d6 = ' '.join(v6)

    from dataanalysis import dataanalysis as da
    tfidf = da.get_tfidf_docs((d1, d2, d3, d4, d5, d6))
    sparse_r1 = tfidf.getrow(0)
    sparse_r2 = tfidf.getrow(1)
    sparse_r3 = tfidf.getrow(2)
    sparse_r4 = tfidf.getrow(3)
    sparse_r5 = tfidf.getrow(4)
    sparse_r6 = tfidf.getrow(5)

    dense_r1 = sparse_r1.todense()
    dense_r2 = sparse_r2.todense()
    dense_r3 = sparse_r3.todense()
    dense_r4 = sparse_r4.todense()
    dense_r5 = sparse_r5.todense()
    dense_r6 = sparse_r6.todense()

    from scipy import spatial
    cs = 1 - spatial.distance.cosine(dense_r1, dense_r2)
    print("1-2: " + str(cs))
    cs = 1 - spatial.distance.cosine(dense_r1, dense_r3)
    print("1-3: " + str(cs))
    cs = 1 - spatial.distance.cosine(dense_r1, dense_r4)
    print("1-4: " + str(cs))
    cs = 1 - spatial.distance.cosine(dense_r1, dense_r5)
    print("1-5: " + str(cs))
    cs = 1 - spatial.distance.cosine(dense_r1, dense_r6)
    print("1-6: " + str(cs))

    cs = 1 - spatial.distance.cosine(dense_r2, dense_r3)
    print("2-3: " + str(cs))
    cs = 1 - spatial.distance.cosine(dense_r2, dense_r4)
    print("2-4: " + str(cs))
    cs = 1 - spatial.distance.cosine(dense_r2, dense_r5)
    print("2-5: " + str(cs))
    cs = 1 - spatial.distance.cosine(dense_r2, dense_r6)
    print("2-6: " + str(cs))

    cs = 1 - spatial.distance.cosine(dense_r3, dense_r4)
    print("3-4: " + str(cs))
    cs = 1 - spatial.distance.cosine(dense_r3, dense_r5)
    print("3-5: " + str(cs))
    cs = 1 - spatial.distance.cosine(dense_r3, dense_r6)
    print("3-6: " + str(cs))

    cs = 1 - spatial.distance.cosine(dense_r4, dense_r5)
    print("4-5: " + str(cs))
    cs = 1 - spatial.distance.cosine(dense_r4, dense_r6)
    print("4-6: " + str(cs))

    cs = 1 - spatial.distance.cosine(dense_r5, dense_r6)
    print("5-6: " + str(cs))

    ## Check vectors from signatures with 100 term

    v1 = ['gregori', 'levet', 'brouillett', 'fravel', 'susskind', 'nanci', 'ayumi', 'm', 'correa', 'faeri', 'david', 'fernandez', 'sadock', 'dagmar', 'murga', 'ben', 'mehrotra', 'kardar', 'twardowski', 'lissett', 'alar', 'ian', 'grove', 'judith', 'thoma', 'culot', 'mikel', 'geraldin', 'patricia', 'friedman', 'tegmark', 'sarah', 'grimm', 'orit', 'movassaghi', 'kemp', 'autor', 'chakraborti', 'daniel', 'georg', 'parag', 'paul', 'lee', 'grave', 'mariusz', 'harri', 'patrick', 'soto', 'roi', 'wallon', 'stroock', 'levitov', 'mehran', 'crocker', 'terri', 'johann', 'hadjiconstantin', 'ellen', 'kedar', 'reinhard', 'joseph', 'peter', 'arian', 'edward', 'layzer', 'jaeger', 'thompson', 'jame', 'nagatomi', 'kocur', 'jung', 'nichola', 'szold', 'michael', 'wornel', 'akintund', 'dunphi', 'jae', 'john', 'goethert', 'paulin', 'min', 'chri', 'inam', 'steven', 'sabin', 'chiang', 'spirn', 'max', 'denni', 'aseem', 'hosoi', 'j', 'ann', 'toomr', 'robert', 'nondita', 'gang', 'stephen', 'maier']
    v2 = ['nannaji', 'eduardo', 'rabbat', 'arindam', 'ceyer', 'janet', 'dow', 'william', 'stewart', 'haruhiko', 'reiner', 'david', 'fernandez', 'tarkowski', 'leist', 'erika', 'klau', 'ghoniem', 'hadjiconstantin', 'gill', 'sodini', 'charl', 'henri', 'mari', 'bath', 'schrenk', 'troxel', 'joan', 'haywood', 'saka', 'ahm', 'caso', 'gibbon', 'mitchel', 'kausel', 'dubowski', 'asada', 'hemond', 'qingyan', 'millon', 'frederick', 'rene', 'pratt', 'naginski', 'hashemi', 'bale', 'cynthia', 'nichola', 'garri', 'john', 'ernst', 'mark', 'watenpaugh', 'carl', 'jame', 'lallit', 'jona', 'brisson', 'jarzombek', 'boyc', 'michael', 'robert', 'nasser', 'belang', 'cravalho', 'ernest', 'mc', 'chen', 'christin', 'yanna', 'h', 'donald', 'steven', 'sylvia', 'zemba', 'krzysztof', 'ioanni', 'harold', 'hardt', 'heghnar', 'j', 'anand', 'wodiczko', 'w', 'joe', 'stephen', 'dutta']
    v3 = ['henni', 'william', 'carmichael', 'baggero', 'jean', 'koster', 'sanyal', 'bishwapriya', 'david', 'kenneth', 'linda', 'burchfiel', 'sonenberg', 'jackson', 'cohen', 'chorov', 'zhiyuan', 'charl', 'judith', 'thoma', 'jo', 'richard', 'edward', 'wheaton', 'gyftopoulo', 'mujid', 'caso', 'allen', 'vandiv', 'daniel', 'georg', 'leonard', 'merrick', 'perair', 'janet', 'paul', 'alan', 'rubner', 'dick', 'harri', 'bernhardt', 'peterson', 'hoon', 'fitzgerald', 'stewart', 'guttag', 'frederick', 'freidberg', 'rene', 'reinhard', 'wilson', 'sibel', 'peter', 'cynthia', 'bozdogan', 'sue', 'john', 'mohr', 'mark', 'ferreira', 'jame', 'tuller', 'wedgwood', 'kazimi', 'michael', 'hein', 'sucharewicz', 'blankschtein', 'welsh', 'jaim', 'schneider', 'blackmer', 'goethert', 'chen', 'arthur', 'magnanti', 'loui', 'yue', 'donald', 'steven', 'murrai', 'wuensch', 's', 'susan', 'genannt', 'riddiough', 'argon', 'essigmann', 'e', 'j', 'ann', 'donaldson', 'gould', 'hine', 'cui', 'amarasingh', 'abelmann', 'robert', 'orlin', 'brown']
    v4 = ['gregori', 'tonegawa', 'herbert', 'linn', 'eduardo', 'roylanc', 'rene', 'david', 'wilson', 'c', 'peter', 'jack', 'kenneth', 'nigel', 'john', 'schneider', 'hobb', 'jame', 'sewel', 'alvin', 'cohen', 's', 'joshua', 'michael', 'hein', 'thoma', 'richard', 'mc', 'susumu', 'caso', 'arthur', 'chen', 'temin', 'roger', 'g', 'samuel', 'donald', 'jeffrei', 'daniel', 'georg', 'kausel', 'susan', 'sawin', 'scott', 'alan', 't', 'm', 'harri', 'peirson', 'ronald', 'w', 'bruce', 'smith']
    v5 = ['levet', 'crocker', 'richard', 'fintel', 'dunphi', 'ellen', 'elizabeth', 'iren', 'totten', 'sanyal', 'samuel', 'bishwapriya', 'peter', 'jaim', 'resnick', 'von', 'paradi', 'perair', 'jane', 'jenkin', 'burn', 'jame', 'sabin', 'lawrenc', 'margeri', 'peirson', 'charl', 'dan', 'monika', 'henri', 'robert', 'liu', 'michael', 'yuehua', 'kai', 'stewart']
    v6 = ['miller', 'hu', 'chung', 'ochsendorf', 'ross', 'chen', 'rose', 'zhao', 'doyl', 'sussman', 'perez', 'sanyal', 'dedon', 'william', 'johnson', 'grossman', 'zhang', 'hart', 'von', 'paxson', 'van', 'yue', 'singer', 'sarma', 'kaiser', 'gruber', 'lee', 'bowr', 'belcher', 'schulz', 'cohen', 'white', 'freeman', 'leiserson', 'schuh', 'wang', 'thompson', 'winston', 'lozano', 'ting', 'stephanopoulo', 'smith', 'verghes']

    d1 = ' '.join(v1)
    d2 = ' '.join(v2)
    d3 = ' '.join(v3)
    d4 = ' '.join(v4)
    d5 = ' '.join(v5)
    d6 = ' '.join(v6)

    from dataanalysis import dataanalysis as da
    tfidf = da.get_tfidf_docs((d1, d2, d3, d4, d5, d6))
    sparse_r1 = tfidf.getrow(0)
    sparse_r2 = tfidf.getrow(1)
    sparse_r3 = tfidf.getrow(2)
    sparse_r4 = tfidf.getrow(3)
    sparse_r5 = tfidf.getrow(4)
    sparse_r6 = tfidf.getrow(5)

    dense_r1 = sparse_r1.todense()
    dense_r2 = sparse_r2.todense()
    dense_r3 = sparse_r3.todense()
    dense_r4 = sparse_r4.todense()
    dense_r5 = sparse_r5.todense()
    dense_r6 = sparse_r6.todense()

    from scipy import spatial
    cs = 1 - spatial.distance.cosine(dense_r1, dense_r2)
    print("1-2: " + str(cs))
    cs = 1 - spatial.distance.cosine(dense_r1, dense_r3)
    print("1-3: " + str(cs))
    cs = 1 - spatial.distance.cosine(dense_r1, dense_r4)
    print("1-4: " + str(cs))
    cs = 1 - spatial.distance.cosine(dense_r1, dense_r5)
    print("1-5: " + str(cs))
    cs = 1 - spatial.distance.cosine(dense_r1, dense_r6)
    print("1-6: " + str(cs))

    cs = 1 - spatial.distance.cosine(dense_r2, dense_r3)
    print("2-3: " + str(cs))
    cs = 1 - spatial.distance.cosine(dense_r2, dense_r4)
    print("2-4: " + str(cs))
    cs = 1 - spatial.distance.cosine(dense_r2, dense_r5)
    print("2-5: " + str(cs))
    cs = 1 - spatial.distance.cosine(dense_r2, dense_r6)
    print("2-6: " + str(cs))

    cs = 1 - spatial.distance.cosine(dense_r3, dense_r4)
    print("3-4: " + str(cs))
    cs = 1 - spatial.distance.cosine(dense_r3, dense_r5)
    print("3-5: " + str(cs))
    cs = 1 - spatial.distance.cosine(dense_r3, dense_r6)
    print("3-6: " + str(cs))

    cs = 1 - spatial.distance.cosine(dense_r4, dense_r5)
    print("4-5: " + str(cs))
    cs = 1 - spatial.distance.cosine(dense_r4, dense_r6)
    print("4-6: " + str(cs))

    cs = 1 - spatial.distance.cosine(dense_r5, dense_r6)
    print("5-6: " + str(cs))


def test_g_prim():
    # create store handler
    store_client = StoreHandler()
    # read graph
    path = 'test/network.pickle'
    network = fieldnetwork.deserialize_network(path)
    api = API(network)
    api.init_store()

    field = ('Mit_student_directory.csv', 'Full Name')
    print("")
    print("Relations of: " + str(field))
    print("")
    print("Content sim")
    print("")

    nodes = api.similar_content_to_field(field)
    for node in nodes:
        print(node)


def test_functions():
    ## Prepare
    # create store handler
    store_client = StoreHandler()
    # read graph
    path = 'test/network.pickle'
    network = fieldnetwork.deserialize_network(path)
    api = API(network)
    api.init_store()


    ## join_path
    print("Join path")
    field1 = ("Drupal_employee_directory.csv", "Full Name")
    field2 = ("Employee_directory.csv", "Full Name Uppercase")
    res = api.join_path(field1, field2)
    api.output(res)

    ## Add column

    print("Add column")
    sn = "Fclt_organization.csv"
    list_of_results = api.schema_complement(sn)
    for l in list_of_results:
        print(str(l))

if __name__ == '__main__':

    #test_all()
    #test()
    #test_g_prim()
    test_functions()
