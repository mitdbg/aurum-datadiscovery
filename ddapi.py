from modelstore.elasticstore import StoreHandler
from modelstore.elasticstore import KWType
from knowledgerepr.fieldnetwork import Relation
from knowledgerepr import fieldnetwork


store_client = None


class DDPrimitiveAPI:

    __network = None

    def __init__(self, network):
        self.__network = network

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


class DDCombinerAPI:

    __network = None

    def __init__(self, network):
        self.__network = network

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


class DDFunctionAPI:

    __network = None

    def __init__(self, network):
        self.__network = network

    def join_path(self, source, target):
        first_class_path = self.__network.find_path(source, target, Relation.PKFK)
        second_class_path = self.__network.find_path(source, target, Relation.CONTENT_SIM)
        path = ([].extend(first_class_path)).extend(second_class_path)
        return path

    def add_columns(self, source_name):
        print("TODO")
        # Retrieve all fields of the given source

        # Find if there are pkfk connections for any of the columns

        # Find if there are any content_sim connection

        # Put them all together

    def add_rows(self):
        print("TODO")

    def fill_schema(self):
        print("TODO")
        # Find set of schema_sim for each field provided in the virtual schema

        # Find the most suitable table, by checking which of the previous fields are schema-connected,
        # and selecting the set with the biggest size.

        # Use table as seed. Find PKFK for any of the fields in the table, that may join to the other
        # requested attributes


class API(DDPrimitiveAPI, DDFunctionAPI, DDCombinerAPI):

    def __init__(self, *args, **kwargs):
        super(API, self).__init__(*args, **kwargs)
        DDCombinerAPI.__init__(self, *args, **kwargs)
        DDFunctionAPI.__init__(self, *args, **kwargs)

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
    #n1 = Node('Hr_faculty_roster.csv', 'First Name')
    #n2 = Node('Iap_subject_person.csv', 'Person Name')
    #n3 = Node('Hr_faculty_roster.csv', 'Directory Org Unit Title')
    results1 = api.kw_search("Michael")
    results2 = api.kw_search("Barbara")
    path = api.in_context_with(results1, results2, Relation.SCHEMA)
    for el in path:
        print(str(el))
