from algebra import API


class FQA:

    def __init__(self, network=None, store_client=None):
        self.api = API(network=network, store_client=store_client)

    def qa(self, entity=None, attribute=None):
        # Find all tables-columns in which entity appears
        entity_matches = self.api.search_content(entity)
        entity_matches.set_table_mode()
        candidate_tables = [table for table in entity_matches]

        # Find the set of attributes that match the requested one
        matching_attributes = self.api.search_attribute(attribute)

        # Find all the attributes that join with each of the previous tables
        entity_table_matching_attribute_candidate = []
        for ctable in candidate_tables:
            all_joinable_tables = self.api.pkfk_of(ctable)
            entity_attr_match = self.api.intersection(all_joinable_tables, matching_attributes)
            if len(entity_attr_match.data) > 0:
                entity_table_matching_attribute_candidate.append((ctable, entity_attr_match))

        # Identify all the cases in which we have entity and a joinable matchable attribute
        for ctable, entity_attr_match in entity_table_matching_attribute_candidate:
            # Check if the join
            a = 1

            # Check if they are materializable

        # Materialize through the filter and offer answers along with context

        return


if __name__ == "__main__":
    print("Fact QA")

    from knowledgerepr import fieldnetwork
    from modelstore.elasticstore import StoreHandler

    # basic test
    # path_to_serialized_model = "/Users/ra-mit/development/discovery_proto/models/newmitdwh/"
    path_to_serialized_model = "/Users/ra/dev/aurum-datadiscovery/models/mit_small/"
    store_client = StoreHandler()
    network = fieldnetwork.deserialize_network(path_to_serialized_model)

    fqa = FQA(network=network, store_client=store_client)
