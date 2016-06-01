from modelstore.elasticstore import StoreHandler
from knowledgerepr import fieldnetwork
from knowledgerepr import networkbuilder
from knowledgerepr.networkbuilder import FieldNetwork


def main():
    network = FieldNetwork()
    store = StoreHandler()

    # Get all fields from store
    fields = store.get_all_fields()
    # Schema relation
    networkbuilder.build_schema_relation(network, fields)
    # Schema_sim relation
    networkbuilder.build_schema_sim_relation(network, fields)
    # Entity_sim relation
    fields, entities = store.get_all_fields_entities()
    networkbuilder.build_entity_sim_relation(network, fields, entities)
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

    print("TODO")

if __name__ == "__main__":
    print("TODO")