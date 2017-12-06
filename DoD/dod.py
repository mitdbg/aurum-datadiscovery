from algebra import API
from api.apiutils import Relation
from enum import Enum
from collections import defaultdict
from collections import OrderedDict
import itertools

api = API()


class FilterType(Enum):
    ATTR = 0
    CELL = 1


def virtual_schema(list_attributes: [str], list_samples: [str]):

    # Align schema definition and samples
    assert len(list_attributes) == len(list_samples)
    sch_def = {attr: value for attr, value in zip(list_attributes, list_samples)}

    # Obtain sets that fulfill individual filters
    filter_drs = dict()
    for attr in sch_def.keys():
        drs = api.search_attribute(attr)
        filter_drs[(attr, FilterType.ATTR)] = drs

    for cell in sch_def.values():
        drs = api.search_content(cell)
        filter_drs[(cell, FilterType.CELL)] = drs

    # We group now into groups that convey multiple filters.
    # Obtain list of tables ordered from more to fewer filters.
    table_fulfilled_filters = defaultdict(list)
    for filter, drs in filter_drs.items():
        for table in drs.set_table_mode():
            table_fulfilled_filters[table].append(filter)
    # sort by value len -> # fulfilling filters
    table_fulfilled_filters = OrderedDict(sorted(table_fulfilled_filters, key=lambda k, v: len(v), reverse=True))

    # Find all combinations of tables...
    # Set cover problem, but enumerating all candidates, not just the minimum size set that covers the universe
    candidate_groups = set()
    num_tables = len(table_fulfilled_filters)
    while num_tables > 0:
        combinations = itertools.combinations(list(table_fulfilled_filters.keys()), num_tables)
        for combination in combinations:
            candidate_groups.add(frozenset(combination))
        num_tables -= 1

    # ...and order by coverage of filters
    candidate_group_filters = defaultdict(list)
    for candidate_group in candidate_groups:
        filter_set = set()
        for table in candidate_group:
            for filter_covered_by_table in table_fulfilled_filters[table]:
                filter_set.add(filter_covered_by_table)
        candidate_group_filters[candidate_group] = list(filter_set)
    candidate_group_filters = OrderedDict(sorted(candidate_group_filters, key=lambda k, v: len(v), reverse=True))

    # Now do all-pairs join paths for each group, and eliminate groups that do not join (future -> transform first)
    joinable_groups = []
    for candidate_group, filters in candidate_group_filters.items():
        if len(candidate_group) > 1:
            join_paths = joinable(candidate_group)
            if join_paths > 0:
                joinable_groups.append((join_paths, filters))
        else:
            joinable_groups.append((candidate_group, filters))  # join not defined on a single table, so we add it

    return joinable_groups


def joinable(group_tables: [str]):
    """
    Check whether all the tables in the group can be part of a 'single' join path
    :param group_tables:
    :return: if yes, a list of valid join paths, if not an empty list
    """
    assert len(group_tables) > 1

    # if we had connected components info, we could check right away whether these are connectable or not

    # Algo
    join_paths = []
    draw = list(group_tables)  # we can add nodes from the join paths
    remaining_nodes = set(group_tables)  # if we empty this, success

    go_on = True
    while go_on:
        table1 = draw[0]  # choose always same until exhausted
        table2 = remaining_nodes.pop()
        t1 = api.make_drs(table1)
        t2 = api.make_drs(table2)
        t1.set_table_mode()
        t2.set_table_mode()
        drs = api.paths(t1, t2, Relation.PKFK, max_hops=3)
        paths = drs.paths()  # list of lists
        if len(paths) > 0:
            for path in paths:  # list of Hits
                for hop in path:
                    intermediate_node = hop.source_name
                    if intermediate_node in remaining_nodes:
                        remaining_nodes.remove(intermediate_node)  # found one
                        join_paths.append(path)  # the join path is useful to cover new nodes
                    elif intermediate_node not in draw:
                        draw.append(intermediate_node)  # intermediate that can join to other nodes later
        else:
            draw.remove(table1)  # this one does not connect to more elements anymore

    if len(remaining_nodes) > 0:
        return join_paths
    else:
        return []  # although we found join paths they did not cover all nodes

if __name__ == "__main__":
    print("DoD")
