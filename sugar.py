# Ignore in-table results of neighbor searches
# Exclude certain tables
# keyword_search and neighbor_search, but on mutiple contexts

import networkx as nx
from api.apiutils import Relation

from modelstore.elasticstore import StoreHandler, KWType
from knowledgerepr import fieldnetwork
from algebra import API

path_to_serialized_model = "/Users/arcarter/code/datadiscovery/test/testmodel/"
network = fieldnetwork.deserialize_network(path_to_serialized_model)
store_client = StoreHandler()

api = API(network, store_client)


# short variables for Scope
# These are used in keyword searches
# To specify what parts of a file will be searched
source = KWType.KW_TABLE  # table/file/source name
field = KWType.KW_SCHEMA  # colum names/fields
content = KWType.KW_TEXT  # content of the columns

# Short variables for Relation
# These represent edge types in the graph
# and are used for neighbor searches
schema = Relation.SCHEMA  # similar schemas
schema_sim = Relation.SCHEMA_SIM  # Similar Schema Names
# similar content values. i.e. matching substrings and numbers
content_sim = Relation.CONTENT_SIM
entity_sim = Relation.ENTITY_SIM  # similar column names
pkfk = Relation.PKFK  # join candidates

# Short variables for api
# These are algebraic functions that can be used to discover data
keyword_search = api.keyword_search
neighbor_search = api.neighbor_search
paths = api.paths
traverse = api.traverse
intersection = api.intersection
union = api.union
difference = api.difference


def search(kws, contexts=[source, field, content]):
    if isinstance(contexts, KWType):
        contexts = [contexts]
    drs = None
    if isinstance(kws, str):
        kws = [kws]

    for kw in kws:
        for kwtype in contexts:
            new_drs = keyword_search(kw, kwtype, max_results=500)
            drs = union(drs, new_drs)
    return drs


def neighbors(i_drs, relations=Relation, exclude_origin=True):
    o_drs = None

    if relations is None:
        relations = [pkfk, content_sim, schema_sim]
    if isinstance(relations, Relation):
        relations = [relations]

    for relation in relations:
        new_drs = neighbor_search(i_drs, relation)
        o_drs = union(o_drs, new_drs)

    if exclude_origin:
        o_drs = difference(o_drs, i_drs)

    return o_drs


def paths_between(a, b, primitive):
    pass


def provenance(i_drs):
    return i_drs.get_provenance().prov_graph().edges()


def make_drs(general_input):
    o_drs = api._general_to_drs(general_input)
    return o_drs


if __name__ == '__sugar__':
    pass
