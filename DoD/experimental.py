from collections import defaultdict
from collections import OrderedDict
import itertools
from DoD.utils import FilterType


def virtual_schema_exhaustive_search(self, list_attributes: [str], list_samples: [str]):
    # Align schema definition and samples
    assert len(list_attributes) == len(list_samples)
    sch_def = {attr: value for attr, value in zip(list_attributes, list_samples)}

    # Obtain sets that fulfill individual filters
    filter_drs = dict()
    for attr in sch_def.keys():
        drs = self.api.search_attribute(attr)
        filter_drs[(attr, FilterType.ATTR)] = drs

    for cell in sch_def.values():
        drs = self.api.search_content(cell)
        filter_drs[(cell, FilterType.CELL)] = drs

    # We group now into groups that convey multiple filters.
    # Obtain list of tables ordered from more to fewer filters.
    table_fulfilled_filters = defaultdict(list)
    for filter, drs in filter_drs.items():
        drs.set_table_mode()
        for table in drs:
            table_fulfilled_filters[table].append(filter)
    # sort by value len -> # fulfilling filters
    a = sorted(table_fulfilled_filters.items(), key=lambda el: len(el[1]), reverse=True)
    table_fulfilled_filters = OrderedDict(
        sorted(table_fulfilled_filters.items(), key=lambda el: len(el[1]), reverse=True))

    # Find all combinations of tables...
    # Set cover problem, but enumerating all candidates, not just the minimum size set that covers the universe
    candidate_groups = set()
    num_tables = len(table_fulfilled_filters)
    while num_tables > 0:
        combinations = itertools.combinations(list(table_fulfilled_filters.keys()), num_tables)
        for combination in combinations:
            candidate_groups.add(frozenset(combination))
        num_tables = num_tables - 1

    # ...and order by coverage of filters
    candidate_group_filters = defaultdict(list)
    for candidate_group in candidate_groups:
        filter_set = set()
        for table in candidate_group:
            for filter_covered_by_table in table_fulfilled_filters[table]:
                filter_set.add(filter_covered_by_table)
        candidate_group_filters[candidate_group] = list(filter_set)
    candidate_group_filters = OrderedDict(
        sorted(candidate_group_filters.items(), key=lambda el: len(el[1]), reverse=True))

    # Now do all-pairs join paths for each group, and eliminate groups that do not join (future -> transform first)
    joinable_groups = []
    for candidate_group, filters in candidate_group_filters.items():
        if len(candidate_group) > 1:
            join_paths = self.joinable(candidate_group)
            if join_paths > 0:
                joinable_groups.append((join_paths, filters))
        else:
            joinable_groups.append((candidate_group, filters))  # join not defined on a single table, so we add it

    return joinable_groups