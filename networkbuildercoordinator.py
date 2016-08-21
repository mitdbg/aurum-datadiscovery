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
    fields = [(nid, fn, sn) for (nid, fn, sn, tv, uv) in fields]
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

    # Content_sim text relation
    start_text_sig_sim = time.time()
    fields, text_signatures = store.get_all_fields_textsignatures()
    networkbuilder.build_content_sim_relation_text(network, fields, text_signatures)
    end_text_sig_sim = time.time()
    print("Total text-sig-sim: {0}".format(str(end_text_sig_sim - start_text_sig_sim)))

    # Content_sim num relation
    start_num_sig_sim = time.time()
    fields, num_signatures = store.get_all_fields_numsignatures()
    networkbuilder.build_content_sim_relation_num(network, fields, num_signatures)
    end_num_sig_sim = time.time()
    print("Total text-sig-sim: {0}".format(str(end_num_sig_sim - start_num_sig_sim)))

    # Primary Key / Foreign key relation
    start_pkfk = time.time()
    networkbuilder.build_pkfk_relation(network)
    end_pkfk = time.time()
    print("Total PKFK: {0}".format(str(end_pkfk - start_pkfk)))

    #topk = 100
    #degree = network.fields_degree(topk)
    #for node, val in degree:
    #    print("N - " + str(node) + " degree: " + str(val))

    #import networkx as nx
    #from matplotlib.pyplot import show
    #nx.write_gml(network._get_underlying_repr(), "gexfTEST.gml")
    #nx.draw(network._get_underlying_repr())
    #show()

    end_all = time.time()
    print("Total time: {0}".format(str(end_all - start_all)))

    """

    # Overlap relation
    networkbuilder.build_overlap_relation()
    # PKFK relation
    networkbuilder.build_pkfk_relation()

    """

    path = "test/partialchembl.pickle"
    fieldnetwork.serialize_network(network, path)

    print("DONE!")


def test():
    network = FieldNetwork()
    store = StoreHandler()

    """
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
    """

    """
    # Content_sim num relation
    start_num_sig_sim = time.time()
    fields, num_signatures = store.get_all_fields_numsignatures()
    networkbuilder.build_content_sim_relation_num(network, fields, num_signatures)
    end_num_sig_sim = time.time()
    print("Total text-sig-sim: {0}".format(str(end_num_sig_sim - start_num_sig_sim)))
    """

    # Content_sim text relation
    start_text_sig_sim = time.time()
    fields, text_signatures = store.get_all_fields_textsignatures()

    num = [x for x in fields]
    print("NUM: " + str(len(num)))

    for i in range(len(fields)):
        print(str(fields[i]) + " -> " + str(text_signatures[i]))

    networkbuilder.build_content_sim_relation_text(network, fields, text_signatures)
    end_text_sig_sim = time.time()
    print("Total text-sig-sim: {0}".format(str(end_text_sig_sim - start_text_sig_sim)))


    import networkx as nx
    from matplotlib.pyplot import show
    # nx.write_gml(network._get_underlying_repr(), "gexfTEST.gml")
    nx.draw(network._get_underlying_repr())
    show()


def plot_num():
    network = FieldNetwork()
    store = StoreHandler()
    fields, num_signatures = store.get_all_fields_numsignatures()

    xaxis = []
    yaxis = []
    numpoints = 0
    for x,y in num_signatures:
        numpoints = numpoints + 1
        xaxis.append(x)
        yaxis.append(y)
    print("Num points: " + str(numpoints))
    import matplotlib.pyplot as plt
    plt.plot(xaxis, yaxis, 'ro')
    plt.axis([0, 600000, 0, 600000])
    #plt.axis([0, 10000, 0, 10000])
    #plt.axis([0, 500, 0, 500])
    plt.show()


def test_cardinality_propagation():
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

    # Content_sim text relation
    start_text_sig_sim = time.time()
    fields, text_signatures = store.get_all_fields_textsignatures()
    networkbuilder.build_content_sim_relation_text(network, fields, text_signatures)
    end_text_sig_sim = time.time()
    print("Total text-sig-sim: {0}".format(str(end_text_sig_sim - start_text_sig_sim)))

    # Content_sim num relation
    start_num_sig_sim = time.time()
    fields, num_signatures = store.get_all_fields_numsignatures()
    networkbuilder.build_content_sim_relation_num(network, fields, num_signatures)
    end_num_sig_sim = time.time()
    print("Total text-sig-sim: {0}".format(str(end_num_sig_sim - start_num_sig_sim)))

    # Primary Key / Foreign key relation
    start_pkfk = time.time()
    networkbuilder.build_pkfk_relation(network)
    end_pkfk = time.time()
    print("Total PKFK: {0}".format(str(end_pkfk - start_pkfk)))

    network.enumerate_pkfk()

    """
    nxr = network._get_underlying_repr()
    nodes = nxr.nodes(data=True)
    total_candidates = 0
    for n, d in nodes:
        c = network.get_cardinality_of(n)
        print(str(n) + " - " + str(c))
        if c > 0.7:
            total_candidates = total_candidates + 1
    print(str("total candidates: " + str(total_candidates)))
    """

    #sn = 'Sis_subject_code.csv'
    #fn = 'Subject Code Desc'
    #from knowledgerepr.fieldnetwork import Relation
    #output = network.neighbors((sn, fn), Relation.PKFK)
    #for o in output:
    #    print(str(o))


if __name__ == "__main__":
    #main()
    #test()
    #plot_num()
    test_cardinality_propagation()
