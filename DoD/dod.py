from algebra import API
from api.apiutils import Relation
from enum import Enum
from collections import defaultdict
from collections import OrderedDict
import itertools
from DoD import data_processing_utils as dpu


class FilterType(Enum):
    ATTR = 0
    CELL = 1


class DoD:

    def __init__(self, network, store_client):
        self.api = API(network=network, store_client=store_client)

    def virtual_schema_iterative_search(self, list_attributes: [str], list_samples: [str]):
        # Align schema definition and samples
        assert len(list_attributes) == len(list_samples)
        sch_def = {attr: value for attr, value in zip(list_attributes, list_samples)}

        # Obtain sets that fulfill individual filters
        filter_drs = dict()
        filter_id = 0
        for attr in sch_def.keys():
            drs = self.api.search_attribute(attr)
            filter_drs[(attr, FilterType.ATTR, filter_id)] = drs
            filter_id += 1

        for cell in sch_def.values():
            drs = self.api.search_content(cell)
            filter_drs[(cell, FilterType.CELL, filter_id)] = drs
            filter_id += 1

        # We group now into groups that convey multiple filters.
        # Obtain list of tables ordered from more to fewer filters.
        table_fulfilled_filters = defaultdict(list)
        for filter, drs in filter_drs.items():
            drs.set_table_mode()
            # All these tables fulfill the filter above
            for table in drs:
                # table_fulfilled_filters[table].append(filter)
                if filter[1] == FilterType.ATTR:
                    if filter not in table_fulfilled_filters[table]:
                        table_fulfilled_filters[table].append(((filter[0], None), FilterType.ATTR, filter[2]))
                elif filter[1] == FilterType.CELL:
                    columns = [c for c in drs.data]  # copy
                    for c in columns:
                        if c.source_name == table:  # filter in this column
                            if filter not in table_fulfilled_filters[table]:
                                table_fulfilled_filters[table].append(((filter[0], c.field_name), FilterType.CELL, filter[2]))
        # sort by value len -> # fulfilling filters

        # table_fulfilled_filters = {key: list(value) for key, value in table_fulfilled_filters.items()}
        #table_fulfilled_filters = OrderedDict(sorted(table_fulfilled_filters.items(), key=lambda el: len(el[1]), reverse=True))
        table_fulfilled_filters = OrderedDict(
            sorted(table_fulfilled_filters.items(), key=lambda el:
            len({filter_id for _, _, filter_id in el[1]}), reverse=True))  # length of unique filters

        def eager_candidate_exploration():
            # Eagerly obtain groups of tables that cover as many filters as possible
            not_found = True
            while not_found:
                candidate_group = []
                candidate_group_filters_covered = set()
                for i in range(len(list(table_fulfilled_filters.items()))):
                    table_pivot, filters_pivot = list(table_fulfilled_filters.items())[i]
                    # Eagerly add pivot
                    candidate_group.append(table_pivot)
                    for el in filters_pivot:
                        candidate_group_filters_covered.add(el)
                    # Did it cover all filters?
                    if len(candidate_group_filters_covered) == len(filter_drs.items()):
                        yield (candidate_group, candidate_group_filters_covered)  # early stop
                        # Cleaning
                        candidate_group.clear()
                        candidate_group_filters_covered.clear()
                        continue
                    for j in range(len(list(table_fulfilled_filters.items()))):
                        idx = i + j + 1
                        if idx == len(table_fulfilled_filters.items()):
                            break
                        table, filters = list(table_fulfilled_filters.items())[idx]
                        new_filters = len(set(filters).union(candidate_group_filters_covered)) - len(candidate_group_filters_covered)
                        if new_filters > 0:  # add table only if it adds new filters
                            candidate_group.append(table)
                            for el in filters:
                                candidate_group_filters_covered.add(el)
                                # Did it cover all filters?
                                if len(candidate_group_filters_covered) == len(filter_drs.items()):
                                    yield (candidate_group, candidate_group_filters_covered)  # early stop
                    yield (candidate_group, candidate_group_filters_covered)
                    # Cleaning
                    candidate_group.clear()
                    candidate_group_filters_covered.clear()

        # Find ways of joining together each group
        for candidate_group, candidate_group_filters_covered in eager_candidate_exploration():
            print(candidate_group)
            print(candidate_group_filters_covered)

            join_paths = self.joinable(candidate_group)
            join_paths = self.tx_join_paths_to_pair_hops(join_paths)
            annotated_join_paths = self.annotate_join_paths_with_filter(join_paths, table_fulfilled_filters, candidate_group)

            # TODO: sort join paths by coverage of filters, or obtain them sorted already. can happen after verification
            # TODO: probably need the groups, to see if the group is materializable, and not just the individual jps

            # Check JP materialization
            print("Found " + str(len(annotated_join_paths)) + " candidate join paths")
            # for jp in annotated_join_paths:
            #     print(jp)

            # For each candidate join_path, check whether it can be materialized or not,
            # then show to user (or the other way around)
            valid_join_paths = self.verify_candidate_join_paths(annotated_join_paths)

            print("Found " + str(len(valid_join_paths)) + " materializable join paths")

            break




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
        table_fulfilled_filters = OrderedDict(sorted(table_fulfilled_filters.items(), key=lambda el: len(el[1]), reverse=True))

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
        candidate_group_filters = OrderedDict(sorted(candidate_group_filters.items(), key=lambda el: len(el[1]), reverse=True))

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

    def joinable(self, group_tables: [str]):
        # FIXME: intermediate hops are not gonna be added right now
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
            if len(remaining_nodes) == 0:
                break
            table2 = remaining_nodes.pop()
            if table1 == table2:
                continue  # no need to join this
            t1 = self.api.make_drs(table1)
            t2 = self.api.make_drs(table2)
            t1.set_table_mode()
            t2.set_table_mode()
            drs = self.api.paths(t1, t2, Relation.PKFK, max_hops=2)
            paths = drs.paths()  # list of lists
            if len(paths) > 0:
                for path in paths:  # list of Hits
                    for hop in path:
                        intermediate_node = hop.source_name
                        if intermediate_node in remaining_nodes:
                            remaining_nodes.remove(intermediate_node)  # found one
                        elif intermediate_node not in draw:
                            draw.append(intermediate_node)  # intermediate that can join to other nodes later
                    join_paths.append(path)  # the join path is useful to cover new nodes

            else:
                draw.remove(table1)  # this one does not connect to more elements anymore

        if len(join_paths) > 0:
            return join_paths
        else:
            return []  # although we found join paths they did not cover all nodes

    def format_join_paths(self, join_paths):
        """
        Transform this into something readable
        :param join_paths: [(hit, hit)]
        :return:
        """
        formatted_jps = []
        for jp in join_paths:
            formatted_jp = ""
            for hop in jp:
                hop_str = hop.db_name + "." + hop.source_name + "." + hop.field_name
                if formatted_jp == "":
                    formatted_jp += hop_str
                else:
                    formatted_jp += " -> " + hop_str
            formatted_jps.append(formatted_jp)
        return formatted_jps

    def tx_join_paths_to_pair_hops(self, join_paths):
        """
        1. get join path, 2. annotate with values to check for, then 3. format into [(l,r)]
        :param join_paths:
        :param table_fulfilled_filters:
        :return:
        """
        join_paths_hops = []
        for jp in join_paths:
            jp_hops = []
            pair = []
            for hop in jp:
                pair.append(hop)
                if len(pair) == 2:
                    jp_hops.append(tuple(pair))
                    pair.clear()
                    pair.append(hop)
            # Now remove pairs with pointers within same relation
            jp_hops = [(l, r) for l, r in jp_hops if l.source_name != r.source_name]
            join_paths_hops.append(jp_hops)
        return join_paths_hops

    def annotate_join_paths_with_filter(self, join_paths, table_fulfilled_filters, candidate_group):
        annotated_jps = []
        for jp in join_paths:
            # For each hop
            annotated_jp = []
            for l, r in jp:
                # each filter is a (attr, filter-type)
                # Check if l side is a table in the group or just an intermediary
                if l.source_name in candidate_group:  # it's a table in group, so retrieve filters
                    filters = table_fulfilled_filters[l.source_name]
                else:
                    filters = None  # indicating no need to check filters for intermediary node
                annotated_hop = (filters, l, r)
                annotated_jp.append(annotated_hop)
            annotated_jps.append(annotated_jp)
        return annotated_jps

    def verify_candidate_join_paths(self, annotated_join_paths):
        materializable_join_paths = []
        for annotated_join_path in annotated_join_paths:
            valid = self.verify_candidate_join_path(annotated_join_path)
            if valid:
                materializable_join_paths.append(annotated_join_path)
        return materializable_join_paths

    def verify_candidate_join_path(self, annotated_join_path):
        tree_valid_filters = dict()
        x = 0
        for filters, l, r in annotated_join_path:  # for each hop
            if filters is not None:
                cut = x  # saving cut so we can prune later
                # pre-filter carrying values
                for info, filter_type, filter_id in filters:
                    if filter_type == FilterType.CELL:
                        attribute = info[1]
                        cell_value_specified_by_user = info[0]  # this will always be one (?)
                        keys_l = dpu.find_key_for(l.source_name, l.field_name,
                                                 attribute, cell_value_specified_by_user)
                        # Now update carrying_values with the first filter
                        to_add = set()
                        for x, payload in tree_valid_filters.items():
                            carrying_filters, carrying_values = payload
                            carrying_values = carrying_values.intersection(set(keys_l))
                            if len(carrying_values) > 0:  # if keeps it valid, create branch
                                carrying_filters.add((info, filter_type, filter_id))
                                x += 1
                                to_add.add((x, carrying_filters, carrying_values))
                        # Add the new expansions
                        for x, carrying_filters, carrying_values in to_add:
                            tree_valid_filters[x] = (carrying_filters, carrying_values)
                    elif filter_type == FilterType.ATTR:
                        continue
                # At this point we can discard elements that did not make it through this filter
                for x, payload in tree_valid_filters.items():
                    if x < cut:
                        del tree_valid_filters[x]
            # Now filter with x
            to_add = set()
            cut = x
            for x, payload in tree_valid_filters.items():
                carrying_filters, carrying_values = payload
                values_to_carry = set()
                for carrying_value in carrying_values:
                    vals = dpu.find_key_for(r.source_name, r.field_name, l.field_name, carrying_value)
                    values_to_carry.update(set(vals))
                if len(values_to_carry) > 0:
                    x += 1
                    to_add.add((x, carrying_filters, values_to_carry))  # filters remain unchanged in this case
            # Add new values
            for x, carrying_filters, carrying_values in to_add:
                tree_valid_filters[x] = (carrying_filters, carrying_values)
            # At this point we can discard elements that did not make it through this filter
            for x, payload in tree_valid_filters.items():
                if x < cut:
                    del tree_valid_filters[x]
        # Check if the join path was valid, also retrieve the number of filters covered by this JP
        if len(tree_valid_filters.items()) > 0:
            return True
        else:
            return False






def test_e2e(dod):
    attrs = ["Mit Id", "Krb Name", "Hr Org Unit Title"]
    values = ["968548423", "kimball", "Mechanical Engineering"]

    joinable_groups = dod.virtual_schema_iterative_search(attrs, values)

    print(joinable_groups)


def test_joinable(dod):
    candidate_group = ['Employee_directory.csv', 'Drupal_employee_directory.csv']
    #candidate_group = ['Se_person.csv', 'Employee_directory.csv', 'Drupal_employee_directory.csv']
    #candidate_group = ['Tip_detail.csv', 'Tip_material.csv']
    join_paths = dod.joinable(candidate_group)

    # print("RAW: " + str(len(join_paths)))
    # for el in join_paths:
    #     print(el)

    join_paths = dod.format_join_paths(join_paths)

    print("CLEAN: " + str(len(join_paths)))
    for el in join_paths:
        print(el)


if __name__ == "__main__":
    print("DoD")

    from knowledgerepr import fieldnetwork
    from modelstore.elasticstore import StoreHandler
    # basic test
    path_to_serialized_model = "/Users/ra-mit/development/discovery_proto/models/newmitdwh/"
    store_client = StoreHandler()
    network = fieldnetwork.deserialize_network(path_to_serialized_model)

    dod = DoD(network=network, store_client=store_client)

    test_e2e(dod)

    # test_joinable(dod)
