from modelstore.elasticstore import StoreHandler
from knowledgerepr import fieldnetwork
from knowledgerepr import networkbuilder
from knowledgerepr.networkbuilder import FieldNetwork

import time


def main():
    start_all = time.time()
    network = FieldNetwork()
    store = StoreHandler()

    # Get all fields from store
    fields_gen = store.get_all_fields()
    fields = [f for f in fields_gen]
    # Schema relation
    start_schema = time.time()
    networkbuilder.build_schema_relation(network, fields)
    end_schema = time.time()
    print("Total schema: {0}".format(str(end_schema - start_schema)))

    # Schema_sim relation
    start_schema_sim = time.time()
    networkbuilder.build_schema_sim_relation(network, fields)
    end_schema_sim = time.time()
    print("Total schema-sim: {0}".format(str(end_schema_sim - start_schema_sim)))

    # Entity_sim relation
    start_entity_sim = time.time()
    fields, entities = store.get_all_fields_entities()
    networkbuilder.build_entity_sim_relation(network, fields, entities)
    end_entity_sim = time.time()
    print("Total entity-sim: {0}".format(str(end_entity_sim - start_entity_sim)))

    import networkx as nx
    from matplotlib.pyplot import show
    #nx.write_gml(network._get_underlying_repr(), "gexfTEST.gml")
    nx.draw(network._get_underlying_repr())
    show()

    end_all = time.time()
    print("Total time: {0}".format(str(end_all - start_all)))

    """


    # Content_sim text relation
    fields, text_signatures = store.get_all_fields_textsignatures()
    networkbuilder.build_content_sim_relation_text(network, fields, text_signatures)
    # Content_sim num relation
    fields, num_signatures = store.get_all_fields_numsignatures()
    networkbuilder.build_content_sim_relation_num(network, fields, num_signatures)
    # Overlap relation
    networkbuilder.build_overlap_relation()
    # PKFK relation
    networkbuilder.build_pkfk_relation()

    path = "test/"
    fieldnetwork.serialize_network(network, path)
    """

    print("DONE!")


def test():
    network = FieldNetwork()
    store = StoreHandler()

    # Entity_sim relation
    start_entity_sim = time.time()
    fields, entities = store.get_all_fields_entities()
    networkbuilder.build_entity_sim_relation(network, fields, entities)
    end_entity_sim = time.time()
    print("Total entity-sim: {0}".format(str(end_entity_sim - start_entity_sim)))

    import networkx as nx
    from matplotlib.pyplot import show
    # nx.write_gml(network._get_underlying_repr(), "gexfTEST.gml")
    nx.draw(network._get_underlying_repr())
    show()

if __name__ == "__main__":
    main()
    #test()