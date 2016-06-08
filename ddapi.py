from modelstore.elasticstore import StoreHandler
from modelstore.elasticstore import KWType
from knowledgerepr.fieldnetwork import Node
from knowledgerepr.fieldnetwork import Relation
from knowledgerepr import fieldnetwork


store_client = None


class DDPrimitiveAPI:

    __network = None

    def __init__(self, network):
        self.__network = network

    def kw_search(self, kw):
        return store_client.search_keywords(kw, KWType.KW_TEXT)

    def schema_search(self, kw):
        return store_client.search_keywords(kw, KWType.KW_SCHEMA)

    def entity_search(self, kw):
        return store_client.search_keywords(kw, KWType.KW_ENTITIES)

    def schema_neighbors(self, field):
        nodes = self.__network.neighbors(field, Relation.SCHEMA)
        return nodes

    def schema_sim_fields(self, field):
        nodes = self.__network.neighbors(field, Relation.SCHEMA_SIM)
        return nodes

    def similar_content_fields(self, field):
        nodes = self.__network.neighbors(field, Relation.CONTENT_SIM)
        return nodes

    def similar_entities_fields(self, field):
        nodes = self.__network.neighbors(field, Relation.ENTITY_SIM)
        return nodes

    def overlap_fields(self, field):
        nodes = self.__network.neighbors(field, Relation.OVERLAP)
        return nodes

    def pkfk_fields(self, field):
        nodes = self.__network.neighbors(field, Relation.PKFK)
        return nodes


class DDFunctionAPI:

    def __init__(self):
        print("TODO")

    def join_path(self):
        print("TODO")

    def may_join_path(self):
        print("TODO")


class API(DDPrimitiveAPI, DDFunctionAPI):

    def __init__(self, *args, **kwargs):
        super(API, self).__init__(*args, **kwargs)

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
