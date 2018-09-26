from algebra import API
from api.apiutils import Relation
from collections import defaultdict
from collections import OrderedDict
import itertools
from DoD import data_processing_utils as dpu
from DoD import material_view_analysis as mva
from DoD.utils import FilterType
import numpy as np
from functools import reduce
import operator
import pickle
from tqdm import tqdm


class DoD:

    def __init__(self, network, store_client, csv_separator=","):
        self.aurum_api = API(network=network, store_client=store_client)
        dpu.configure_csv_separator(csv_separator)

    def individual_filters(self, sch_def):
        # Obtain sets that fulfill individual filters
        filter_drs = dict()
        filter_id = 0
        for attr in sch_def.keys():
            drs = self.aurum_api.search_exact_attribute(attr, max_results=200)
            filter_drs[(attr, FilterType.ATTR, filter_id)] = drs
            filter_id += 1

        for cell in sch_def.values():
            drs = self.aurum_api.search_content(cell, max_results=200)
            filter_drs[(cell, FilterType.CELL, filter_id)] = drs
            filter_id += 1
        return filter_drs

    def joint_filters(self, sch_def):
        # Obtain sets that fulfill individual filters
        filter_drs = dict()
        filter_id = 0

        for attr, cell in sch_def.items():
            if cell == "":
                drs = self.aurum_api.search_exact_attribute(attr, max_results=50)
                filter_drs[(attr, FilterType.ATTR, filter_id)] = drs
            else:
                drs_attr = self.aurum_api.search_exact_attribute(attr, max_results=50)
                drs_cell = self.aurum_api.search_content(cell, max_results=500)
                drs = self.aurum_api.intersection(drs_attr, drs_cell)
                filter_drs[(cell, FilterType.CELL, filter_id)] = drs
            filter_id += 1
        return filter_drs

    def virtual_schema_iterative_search(self, list_attributes: [str], list_samples: [str], max_hops=2, debug_enumerate_all_jps=False):
        # Align schema definition and samples
        assert len(list_attributes) == len(list_samples)
        sch_def = {attr: value for attr, value in zip(list_attributes, list_samples)}

        sch_def = OrderedDict(sorted(sch_def.items(), key=lambda x: x[0], reverse=True))

        filter_drs = self.joint_filters(sch_def)

        # We group now into groups that convey multiple filters.
        # Obtain list of tables ordered from more to fewer filters.
        table_fulfilled_filters = defaultdict(list)
        table_nid = dict()  # collect nids -- used later to obtain an access path to the tables
        for filter, drs in filter_drs.items():
            drs.set_table_mode()
            # All these tables fulfill the filter above
            for table in drs:
                # table_fulfilled_filters[table].append(filter)
                if filter[1] == FilterType.ATTR:
                    columns = [c for c in drs.data]  # copy
                    for c in columns:
                        if c.source_name == table:
                            table_nid[table] = c.nid
                    # if filter not in table_fulfilled_filters[table]:
                    if filter[2] not in [id for _,_,id in table_fulfilled_filters[table]]:
                        table_fulfilled_filters[table].append(((filter[0], None), FilterType.ATTR, filter[2]))
                elif filter[1] == FilterType.CELL:
                    columns = [c for c in drs.data]  # copy
                    for c in columns:
                        if c.source_name == table:  # filter in this column
                            table_nid[table] = c.nid
                            # if filter not in table_fulfilled_filters[table]:
                            if filter[2] not in [id for _, _, id in table_fulfilled_filters[table]]:
                                table_fulfilled_filters[table].append(((filter[0], c.field_name), FilterType.CELL, filter[2]))

        table_path = obtain_table_paths(table_nid, self)

        # sort by value len -> # fulfilling filters
        table_fulfilled_filters = OrderedDict(
            sorted(table_fulfilled_filters.items(), key=lambda el:
            (len({filter_id for _, _, filter_id in el[1]}), el[0]), reverse=True))  # len of unique filters, then lexico

        # Ordering filters for more determinism
        for k, v in table_fulfilled_filters.items():
            v = sorted(v, key=lambda el: (el[2], el[0][0]), reverse=True)  # sort by id, then filter_name
            table_fulfilled_filters[k] = v

        def eager_candidate_exploration():
            def covers_filters(candidate_filters, all_filters):
                all_filters_set = set([id for _, _, id in filter_drs.keys()])
                candidate_filters_set = set([id for _, _, id in candidate_filters])
                if len(candidate_filters_set) == len(all_filters_set):
                    return True
                return False

            def compute_size_filter_ix(filters, candidate_group_filters_covered):
                new_fs_set = set([id for _,_,id in filters])
                candidate_fs_set = set([id for _,_,id in candidate_group_filters_covered])
                ix_size = len(new_fs_set.union(candidate_fs_set)) - len(candidate_fs_set)
                return ix_size

            def clear_state():
                candidate_group.clear()
                candidate_group_filters_covered.clear()
            # Eagerly obtain groups of tables that cover as many filters as possible
            go_on = True
            while go_on:
                candidate_group = []
                candidate_group_filters_covered = set()
                for i in range(len(list(table_fulfilled_filters.items()))):
                    table_pivot, filters_pivot = list(table_fulfilled_filters.items())[i]
                    # Eagerly add pivot
                    candidate_group.append(table_pivot)
                    candidate_group_filters_covered.update(filters_pivot)
                    # Did it cover all filters?
                    # if len(candidate_group_filters_covered) == len(filter_drs.items()):
                    if covers_filters(candidate_group_filters_covered, filter_drs.items()):
                        candidate_group = sorted(candidate_group)
                        # print("1: " + str(table_pivot))
                        yield (candidate_group, candidate_group_filters_covered)  # early stop
                        # Cleaning
                        clear_state()
                        continue
                    for j in range(len(list(table_fulfilled_filters.items()))):
                        idx = i + j + 1
                        if idx == len(table_fulfilled_filters.items()):
                            break
                        table, filters = list(table_fulfilled_filters.items())[idx]
                        # new_filters = len(set(filters).union(candidate_group_filters_covered)) - len(candidate_group_filters_covered)
                        new_filters = compute_size_filter_ix(filters, candidate_group_filters_covered)
                        if new_filters > 0:  # add table only if it adds new filters
                            candidate_group.append(table)
                            candidate_group_filters_covered.update(filters)
                            if covers_filters(candidate_group_filters_covered, filter_drs.items()):
                            # if len(candidate_group_filters_covered) == len(filter_drs.items()):
                                candidate_group = sorted(candidate_group)
                                # print("2: " + str(table_pivot))
                                yield (candidate_group, candidate_group_filters_covered)
                                clear_state()
                                # Re-add the current pivot, only necessary in this case
                                candidate_group.append(table_pivot)
                                candidate_group_filters_covered.update(filters_pivot)
                    candidate_group = sorted(candidate_group)
                    # print("3: " + str(table_pivot))
                    yield (candidate_group, candidate_group_filters_covered)
                    # Cleaning
                    clear_state()
                go_on = False  # finished exploring all groups

        # Find ways of joining together each group
        cache_unjoinable_pairs = defaultdict(int)
        for candidate_group, candidate_group_filters_covered in eager_candidate_exploration():
            print("")
            print("Candidate group: " + str(candidate_group))
            num_unique_filters = len({f_id for _, _, f_id in candidate_group_filters_covered})
            print("Covers #Filters: " + str(num_unique_filters))

            if len(candidate_group) == 1:
                table = candidate_group[0]
                path = table_path[table]
                materialized_virtual_schema = dpu.get_dataframe(path + "/" + table)
                attrs_to_project = dpu.obtain_attributes_to_project((candidate_group_filters_covered, None))
                yield materialized_virtual_schema, attrs_to_project
                continue  # to go to the next group

            # Pre-check
            # TODO: with a connected components index we can pre-filter many of those groups without checking
            #group_with_all_relations, join_path_groups = self.joinable(candidate_group, cache_unjoinable_pairs)
            max_hops = max_hops
            # We find the different join graphs that would join the candidate_group
            join_path_groups = self.joinable(candidate_group, cache_unjoinable_pairs, max_hops=max_hops)
            if debug_enumerate_all_jps:
                for i, group in enumerate(join_path_groups):
                    print("Group: " + str(i))
                    for el in group:
                        print(el)
                continue  # We are just interested in all JPs for all candidate groups

            # if not paths or graphs skip next
            if len(join_path_groups) == 0:
                print("Group: " + str(candidate_group) + " is Non-Joinable with max_hops=" + str(max_hops))
                continue

            # Now we need to check every join graph individually and see if it's materializable. Only once we've
            # exhausted these join graphs we move on to the next candidate group. We know already that each of the
            # join graphs covers all tables in candidate_group, so if they're materializable we're good.
            materializable_join_graphs = []
            for jpg, num_hops in join_path_groups:
                # Transform collection of paths into pairs
                join_paths = self.tx_join_paths_to_pair_hops(jpg)
                # Annotate them with filters
                annotated_join_paths = self.annotate_join_paths_with_filter(join_paths,
                                                                            table_fulfilled_filters,
                                                                            candidate_group)

                # Is this join graph materializable?
                # FIXME: that indexing at 0, why is it necessary? are we introducing an unnecesary list before?
                # FIXME: or does this break in certain cases?
                is_join_graph_valid = self.is_join_graph_materializable(annotated_join_paths)

                if is_join_graph_valid:
                    # TODO: we are not propagating the num_hops info, which can be recomputed later, but since
                    # TODO: we have it already... also, it may be the better criteria to rank join graphs
                    materializable_join_graphs.append(annotated_join_paths[0])

            # We have now all the materializable join graphs for this candidate group
            # We can sort them by how likely they use 'keys'
            all_jgs_scores = rank_materializable_join_graphs(materializable_join_graphs, table_path, self)

            # Do some clean up
            clean_jp = []
            for annotated_jp, aggr_score, mul_score in all_jgs_scores:
                jp = []
                filters = set()
                for filter, l, r in annotated_jp:
                    # To drag filters along, there's a leaf special tuple where r may be None
                    # since we don't need it at this point anymore, we check for its existence and do not include it
                    if r is not None:
                        jp.append((l, r))
                    if filter is not None:
                        filters.update(filter)
                clean_jp.append((filters, jp))

            # import pickle
            # with open("check_debug.pkl", 'wb') as f:
            #     pickle.dump(clean_jp, f)

            for mjp in clean_jp:
                attrs_to_project = dpu.obtain_attributes_to_project(mjp)
                # materialized_virtual_schema = dpu.materialize_join_path(mjp, self)
                materialized_virtual_schema = dpu.materialize_join_graph(mjp, self)
                yield materialized_virtual_schema, attrs_to_project

        print("Finished enumerating groups")
        cache_unjoinable_pairs = OrderedDict(sorted(cache_unjoinable_pairs.items(),
                                                    key=lambda x: x[1], reverse=True))
        for k, v in cache_unjoinable_pairs.items():
            print(str(k) + " => " + str(v))


            # print("Processing join graphs...")
            # materializable_join_graphs = dict()
            # for k, v in join_path_groups.items():
            #     print("Pair: " + str(k))
            #     join_paths = self.tx_join_paths_to_pair_hops(v)
            #     annotated_join_paths = self.annotate_join_paths_with_filter(join_paths,
            #                                                                 table_fulfilled_filters,
            #                                                                 candidate_group)
            #
            #     # Check JP materialization
            #     print("Found " + str(len(annotated_join_paths)) + " candidate join paths for join graph")
            #
            #     # For each candidate join_path, check whether it can be materialized or not,
            #     # then show to user (or the other way around)
            #     valid_join_paths = self.verify_candidate_join_paths(annotated_join_paths)
            #
            #     print("Found " + str(len(valid_join_paths)) + " materializable join paths for join graph")
            #
            #     if len(valid_join_paths) > 0:
            #         materializable_join_graphs[k] = valid_join_paths
            #     else:
            #         # This pair is non-materializable, but there may be other groups of pairs that cover
            #         # the same tables, therefore we can only continue, we cannot determine at this point that
            #         # the group is non-materializable, not yet.
            #         continue
            # # Verify whether the join_graphs cover the group or not
            # covered_tables = set(candidate_group)
            # for k, _ in materializable_join_graphs.items():
            #     (t1, t2) = k
            #     if t1 in covered_tables:
            #         covered_tables.remove(t1)
            #     if t2 in covered_tables:
            #         covered_tables.remove(t2)
            # if len(covered_tables) > 0:
            #     # now we know there are not join graphs in this group, so we explicitly mark it as such
            #     materializable_join_graphs.clear()
            #     materializable_join_graphs = list()  # next block of processing expects a list
            # else:
            #     # 1) find key-groups
            #     keygroups = defaultdict(list)
            #     current_id = 0
            #     for keygroup in itertools.combinations(list(materializable_join_graphs.keys()),
            #                                            len(candidate_group) - 1):
            #         for key in keygroup:
            #             keygroups[current_id].append(materializable_join_graphs[key])
            #         current_id += 1
            #
            #     # 2) for each key-group, enumerate all paths
            #     unit_jp = []
            #     for _, keygroup in keygroups.items():
            #         # def unpack(packed_list):
            #         #     for el in packed_list:
            #         #         yield [v[0] for v in el]
            #         args = keygroup
            #         for comb in itertools.product(*args):
            #             unit_jp.append(comb)
            #
            #     # pack units into more compact format
            #     materializable_join_graphs = []  # TODO: note we are rewriting the type of a var in scope
            #     for unit in unit_jp:
            #         packed_unit = []
            #         for el in unit:
            #             packed_unit.append(el[0])
            #         materializable_join_graphs.append(packed_unit)
            # print("Processing join graphs...OK")
            #
            # # # Merge join paths and join graphs, at this point the difference is meaningless
            # # # TODO: are paths necessarily contained in graphs? if so, simplify code above
            # #
            # # all_jgs = materializable_join_graphs + materializable_join_paths
            # all_jgs = materializable_join_graphs
            #
            # print("Processing materializable join paths...")
            #
            # # Sort materializable_join_paths by likely joining on key
            # all_jgs_scores = rank_materializable_join_graphs(all_jgs, table_path, self)
            #
            # clean_jp = []
            # for annotated_jp, aggr_score, mul_score in all_jgs_scores:
            #     jp = []
            #     filters = set()
            #     for filter, l, r in annotated_jp:
            #         # To drag filters along, there's a leaf special tuple where r may be None
            #         # since we don't need it at this point anymore, we check for its existence and do not include it
            #         if r is not None:
            #             jp.append((l, r))
            #         if filter is not None:
            #             filters.update(filter)
            #     clean_jp.append((filters, jp))
            #
            # import pickle
            # with open("check_debug.pkl", 'wb') as f:
            #     pickle.dump(clean_jp, f)
            #
            # for mjp in clean_jp:
            #     attrs_to_project = dpu.obtain_attributes_to_project(mjp)
            #     # materialized_virtual_schema = dpu.materialize_join_path(mjp, self)
            #     materialized_virtual_schema = dpu.materialize_join_graph(mjp, self)
            #     yield materialized_virtual_schema, attrs_to_project

        # print("Finished enumerating groups")
        # cache_unjoinable_pairs = OrderedDict(sorted(cache_unjoinable_pairs.items(),
        #                                             key=lambda x: x[1], reverse=True))
        # for k, v in cache_unjoinable_pairs.items():
        #     print(str(k) + " => " + str(v))

    def joinable(self, group_tables: [str], cache_unjoinable_pairs: defaultdict(int), max_hops=2):
        """
        Find all join graphs that connect the tables in group_tables. This boils down to check
        whether there is (at least) a set of join paths which connect all tables, but it is required to find all
        possible join graphs and not only one.
        :param group_tables:
        :param cache_unjoinable_pairs: this set contains pairs of tables that do not join with each other
        :return:
        """
        assert len(group_tables) > 1

        # if not the size of group_tables, there won't be unique jps with all tables. that may not be good though
        max_hops = max_hops

        # for each pair of tables in group keep list of (path, tables_covered)
        paths_per_pair = defaultdict(list)

        for table1, table2 in itertools.combinations(group_tables, 2):
            # Check if tables are already known to be unjoinable
            if (table1, table2) in cache_unjoinable_pairs.keys() or (table2, table1) in cache_unjoinable_pairs.keys():
                continue
            t1 = self.aurum_api.make_drs(table1)
            t2 = self.aurum_api.make_drs(table2)
            t1.set_table_mode()
            t2.set_table_mode()
            drs = self.aurum_api.paths(t1, t2, Relation.PKFK, max_hops=max_hops)
            paths = drs.paths()  # list of lists
            # If we didn't find paths, update unjoinable_pairs cache with this pair
            if len(paths) == 0:  # then store this info, these tables do not join
                cache_unjoinable_pairs[(table1, table2)] += 1
                cache_unjoinable_pairs[(table2, table1)] += 1
            for p in paths:
                tables_covered = set()
                tables_in_group = set(group_tables)
                for hop in p:
                    if hop.source_name in tables_in_group:
                        # this is a table covered by this path
                        tables_covered.add(hop.source_name)
                paths_per_pair[(table1, table2)].append((p, tables_covered))

        # enumerate all possible join graphs
        join_graphs = []
        all_combinations = [el for el in itertools.product(*list(paths_per_pair.values()))]
        deduplicated_paths = dict()
        for path_combination in all_combinations:
            # path_combination = path_combination[0]  # unpack the combination
            for p1, p2 in itertools.combinations(path_combination, 2):
                path1, tables_covered1 = p1
                path2, tables_covered2 = p2
                # does combining these two paths help to cover more tables?
                if len(tables_covered1) > len(tables_covered2):
                    current_cover_len = len(tables_covered1)
                else:
                    current_cover_len = len(tables_covered2)
                potential_cover = tables_covered1.union(tables_covered2)
                joinable_paths = tables_covered1.intersection(tables_covered2)
                potential_cover_len = len(potential_cover)
                # if we cover more tables, and the paths are joinable (at least one table in common)
                if potential_cover_len > current_cover_len and len(joinable_paths) > 0:
                    # combine the paths
                    combined_path = path1 + path2
                    path_id = frozenset([hop.nid for hop in combined_path])
                    # If I haven't generated this path elsewhere, then I add it along with the tables it covers
                    if path_id not in deduplicated_paths:
                        deduplicated_paths[path_id] = (combined_path, potential_cover)
        # Once all combination paths have been generated, we find
        total_tables_covered = set()
        total_hops = 0
        for path, tables_covered in path_combination:
            if len(tables_covered) == len(group_tables):
                join_graphs.append(([path], len([hop for hop in path])))  # unique path covering all tables
                continue  # we want minimum join graphs, so we skip this one which is covers all tables by itself
            total_tables_covered.update(tables_covered)
            total_hops += len(path)
        # Check if join graph is valid, if not just filter it out
        if len(total_tables_covered) == len(group_tables):
            join_graphs.append(([p for p, tables_covered in path_combination], total_hops))

        # sort join_graph based on the length of the involved hops in each of them
        join_graphs = sorted(join_graphs, key=lambda x: x[1], reverse=False)
        return join_graphs

        # # enumerate all possible join graphs
        # join_graphs = []
        # all_combinations = [el for el in itertools.product(*list(paths_per_pair.values()))]
        # for path_combination in all_combinations:
        #     path_combination = path_combination[0]  # unpack the combination
        #     total_tables_covered = set()
        #     total_hops = 0
        #     for path, tables_covered in path_combination:
        #         if len(tables_covered) == len(group_tables):
        #             join_graphs.append(([path], len([hop for hop in path])))  # unique path covering all tables
        #             continue  # we want minimum join graphs, so we skip this one which is covers all tables by itself
        #         total_tables_covered.update(tables_covered)
        #         total_hops += len(path)
        #     # Check if join graph is valid, if not just filter it out
        #     if len(total_tables_covered) == len(group_tables):
        #         join_graphs.append(([p for p, tables_covered in path_combination], total_hops))
        #
        # # sort join_graph based on the length of the involved hops in each of them
        # join_graphs = sorted(join_graphs, key=lambda x: x[1], reverse=False)
        # return join_graphs

    def _joinable(self, group_tables: [str], cache_unjoinable_pairs: defaultdict(int), max_hops=2):
        """
        Check whether there is join graph that connects the tables in the group. This boils down to check
        whether there is a set of join paths which connect all tables.
        :param group_tables:
        :param cache_unjoinable_pairs: this set contains pairs of tables that do not join with each other
        :return:
        """
        # FIXME: needs to consider the case of having multiple different strategies for joining the group
        assert len(group_tables) > 1

        # Check first with the cache whether these are unjoinable
        for table1, table2 in itertools.combinations(group_tables, 2):
            if (table1, table2) in cache_unjoinable_pairs.keys() or (table2, table1) in cache_unjoinable_pairs.keys():
                # We count the attempt
                cache_unjoinable_pairs[(table1, table2)] += 1
                cache_unjoinable_pairs[(table2, table1)] += 1
                print(table1 + " unjoinable to: " + table2 + " skipping...")
                return [], []

        # if not the size of group_tables, there won't be unique jps with all tables. that may not be good though
        max_hops = max_hops

        join_path_groups_dict = dict()  # store groups, as many as pairs of tables in the group
        for table1, table2 in itertools.combinations(group_tables, 2):
            t1 = self.aurum_api.make_drs(table1)
            t2 = self.aurum_api.make_drs(table2)
            t1.set_table_mode()
            t2.set_table_mode()
            drs = self.aurum_api.paths(t1, t2, Relation.PKFK, max_hops=max_hops)
            paths = drs.paths()  # list of lists
            group = []
            if len(paths) == 0:  # then store this info, these tables do not join
                cache_unjoinable_pairs[(table1, table2)] += 1
                cache_unjoinable_pairs[(table2, table1)] += 1
            path_covers_all_tables = False
            path_covering_all_tables = None
            for p in paths:
                tables_covered = set(group_tables)
                for hop in p:
                    if hop.source_name in tables_covered:
                        tables_covered.remove(hop.source_name)
                if len(tables_covered) == 0:
                    path_covers_all_tables = True
                    path_covering_all_tables = p  # we found a path that covers all tables in the group
                    break
                else:  # We add the path
                    group.append(p)
            if path_covers_all_tables:
                join_path_groups_dict[(table1, table2)] = [path_covering_all_tables]
            else:
                join_path_groups_dict[(table1, table2)] = group

        # Remove invalid combinations from join_path_groups_dict
        # FIXME: this logic depends on non-obvious invariants, such as on the max_hops found above
        for i in range(len(group_tables)):
            invalid = False
            table1 = group_tables[i]
            for table2 in group_tables[i + 1:]:
                if len(join_path_groups_dict[(table1, table2)]) == 0:
                    invalid = True
            if invalid:
                # table 1 does not join to all the others, so no join graph here
                to_remove = set()
                for k in join_path_groups_dict.keys():
                    if k[0] == table1:
                        to_remove.add(k)
                for el in to_remove:
                    del join_path_groups_dict[el]

        # Now verify that it's possible to obtain a join graph from these jps
        if len(join_path_groups_dict.items()) == 0:
            return [], []  # no jps covering all tables and empty groups => no join graph
        return join_path_groups_dict

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
        # FIXME: does this keep *all* relevant filters?
        annotated_jps = []
        l = None  # memory for last hop
        r = None
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
            # Finally we must check if the very last table was also part of the jp, so we can add the filters for it
            if r.source_name in candidate_group:
                filters = table_fulfilled_filters[r.source_name]
                annotated_hop = (filters, r, None)  # r becomes left and we insert a None to indicate the end
                last_hop = annotated_jps[-1]
                last_hop.append(annotated_hop)
        return annotated_jps

    def verify_candidate_join_paths(self, annotated_join_paths):
        materializable_join_paths = []
        total_jps = len(annotated_join_paths)
        i = 0
        for annotated_join_path in annotated_join_paths:
            print("Verifying " + str(i) + "/" + str(total_jps), end="", flush=True)
            valid, filters = self.verify_candidate_join_path(annotated_join_path)
            i += 1
            if valid:
                materializable_join_paths.append(annotated_join_path)
        return materializable_join_paths

    def is_join_graph_materializable(self, annotated_join_paths):
        for idx, annotated_join_path in enumerate(annotated_join_paths):
            valid, filters = self.verify_candidate_join_path(annotated_join_path)
            if not valid:
                return False
        return True

    def verify_candidate_join_path(self, annotated_join_path):
        """
        TODO: what about materializing views of the tables that are filtered by the selection predicates?
        """
        tree_valid_filters = dict()
        x = 0
        for filters, l, r in annotated_join_path:  # for each hop
            l_path = self.aurum_api.helper.get_path_nid(l.nid)
            tree_for_level = dict()

            # Before checking for filters, translate carrying values into hook attribute in l
            if len(tree_valid_filters) != 0:  # i.e., not first hop
                x_to_remove = set()
                for x, payload in tree_valid_filters.items():
                    carrying_filters, carrying_values = payload
                    if carrying_values[0] is None and carrying_values[1] is None:
                        # tree_valid_filters[x] = (carrying_filters, (None, None))
                        continue  # in this case nothing to hook
                    attr = carrying_values[1]
                    if attr == l.field_name:
                        continue  # no need to translate values to hook in this case
                    hook_values = set()
                    for carrying_value in carrying_values[0]:
                        values = dpu.find_key_for(l_path + "/" + l.source_name, l.field_name,
                                                   attr, carrying_value)
                        hook_values.update(values)
                    if len(hook_values) > 0:
                        # FIXME: EXPERIMENTAL HERE!!
                        tree_valid_filters[x] = (carrying_filters, (hook_values, l.field_name))  # update tree
                    else:  # does this even make sense?
                        x_to_remove.add(x)
                for x in x_to_remove:
                    del tree_valid_filters[x]
                if len(tree_valid_filters.items()) == 0:
                    return False, set()

            if filters is None:
                # This means we are in an intermediate hop with no filters, as it's only connecting
                # we still need to hook connect the carrying values
                r_path = self.aurum_api.helper.get_path_nid(r.nid)
                x_to_remove = set()
                for x, payload in tree_valid_filters.items():
                    carrying_filters, carrying_values = payload
                    if carrying_values[0] is None and carrying_values[1] is None:
                        # propagate the None None because all values work
                        tree_for_level[x] = (carrying_filters, (None, None))
                        continue
                    values_to_carry = set()
                    # attr = carrying_values[1]
                    for carrying_value in carrying_values[0]:
                        path = r_path + "/" + r.source_name
                        exists = dpu.is_value_in_column(carrying_value, path, r.field_name)
                        if exists:
                            values_to_carry.add(carrying_value)  # this one checks
                    if len(values_to_carry) > 0:
                        # here we update the tree at the current level
                        tree_for_level[x] = (carrying_filters, (values_to_carry, r.field_name))
                    else:
                        return False, set()  # non joinable, stop trying
                        # x_to_remove.add(x)
                # remove if any
                for x in x_to_remove:
                    del tree_for_level[x]  # no more results here, need to prune
                tree_valid_filters = tree_for_level
                if len(tree_valid_filters.items()) == 0:
                    return False, set()  # early stop
                continue
            if filters is not None:
                # sort filters so cell type come first
                filters = sorted(filters, key=lambda x: x[1].value)
                # pre-filter carrying values
                for info, filter_type, filter_id in filters:
                    if filter_type == FilterType.CELL:
                        attribute = info[1]
                        cell_value_specified_by_user = info[0]  # this will always be one (?) FIXME: no! only when using the pre-interface
                        path = l_path + "/" + l.source_name
                        keys_l = dpu.find_key_for(path, l.field_name,
                                                 attribute, cell_value_specified_by_user)
                        # Check for the first addition
                        if len(tree_valid_filters.items()) == 0:
                            x += 1
                            tree_for_level[x] = ({(info, filter_type, filter_id)}, (set(keys_l), l.field_name))
                        # Now update carrying_values with the first filter
                        for x, payload in tree_valid_filters.items():
                            carrying_filters, carrying_values = payload
                            ix = carrying_values[0].intersection(set(keys_l))
                            if len(ix) > 0:  # if keeps it valid, create branch
                                carrying_filters.add((info, filter_type, filter_id))
                                x += 1
                                tree_for_level[x] = (carrying_filters, (ix, l.field_name))
                    elif filter_type == FilterType.ATTR:
                        # attr filters work with everyone, so just append
                        # Check for the first addition, TODO: tree_for_level ?
                        if len(tree_for_level.items()) == 0:
                            x += 1
                            tree_for_level[x] = ({(info, filter_type, filter_id)}, (None, None))
                        for x, payload in tree_for_level.items():
                            carrying_filters, carrying_values = payload
                            carrying_filters.add((info, filter_type, filter_id))
                            tree_for_level[x] = (carrying_filters, carrying_values)
            # Now filter with r
            if r is not None:  # if none, we processed the last step already, so time to check the tree
                r_path = self.aurum_api.helper.get_path_nid(r.nid)
                x_to_remove = set()
                for x, payload in tree_for_level.items():
                    carrying_filters, carrying_values = payload
                    if carrying_values[0] is None and carrying_values[1] is None:
                        # propagate the None None because all values work
                        tree_for_level[x] = (carrying_filters, (None, None))
                        continue
                    values_to_carry = set()
                    for carrying_value in carrying_values[0]:
                        path = r_path + "/" + r.source_name
                        exists = dpu.is_value_in_column(carrying_value, path, r.field_name)
                        if exists:
                            values_to_carry.add(carrying_value)  # this one checks
                    if len(values_to_carry) > 0:
                        # here we update the tree at the current level
                        tree_for_level[x] = (carrying_filters, (values_to_carry, r.field_name))
                    else:
                        x_to_remove.add(x)
                # remove if any
                for x in x_to_remove:
                    del tree_for_level[x]  # no more results here, need to prune
                tree_valid_filters = tree_for_level
                if len(tree_valid_filters.items()) == 0:
                    return False, set()  # early stop
        # Check if the join path was valid, also retrieve the number of filters covered by this JP
        if len(tree_valid_filters.items()) > 0:
            for k, v in tree_for_level.items():
                tree_valid_filters[k] = v  # merge trees
            unique_filters = set()
            for k, v in tree_valid_filters.items():
                unique_filters.update(v[0])
            return True, len(unique_filters)
        else:
            return False, set()


def rank_materializable_join_graphs(materializable_join_paths, table_path, dod):

    def score_for_key(keys_score, target):
        for c, nunique, score in keys_score:
            if target == c:
                return score

    def aggr_avg(scores):
        scores = np.asarray(scores)
        return np.average(scores)

    def aggr_mul(scores):
        return reduce(operator.mul, scores)

    rank_jps = []
    keys_cache = dict()
    for mjp in materializable_join_paths:
        jump_scores = []
        for filter, l, r in mjp:
            table = l.source_name
            if table not in keys_cache:
                if table not in table_path:
                    nid = (dod.aurum_api.make_drs(table)).data[0].nid
                    path = dod.aurum_api.helper.get_path_nid(nid)
                    table_path[table] = path
                path = table_path[table]
                table_df = dpu.get_dataframe(path + "/" + table)
                likely_keys_sorted = mva.most_likely_key(table_df)
                keys_cache[table] = likely_keys_sorted
            likely_keys_sorted = keys_cache[table]
            jump_score = score_for_key(likely_keys_sorted, l.field_name)
            jump_scores.append(jump_score)
        jp_score_avg = aggr_avg(jump_scores)
        jp_score_mul = aggr_mul(jump_scores)
        rank_jps.append((mjp, jp_score_avg, jp_score_mul))
    rank_jps = sorted(rank_jps, key=lambda x: x[1], reverse=True)
    return rank_jps


def rank_materializable_join_paths_piece(materializable_join_paths, candidate_group, table_path, dod):
    # compute rank list of likely keys for each table
    table_keys = dict()
    table_field_rank = dict()
    for table in candidate_group:
        if table in table_path:
            path = table_path[table]
        else:
            nid = (dod.aurum_api.make_drs(table)).data[0].nid
            path = dod.aurum_api.helper.get_path_nid(nid)
            table_path[table] = path
        table_df = dpu.get_dataframe(path + "/" + table)
        likely_keys_sorted = mva.most_likely_key(table_df)
        table_keys[table] = likely_keys_sorted
        field_rank = {payload[0]: i for i, payload in enumerate(likely_keys_sorted)}
        table_field_rank[table] = field_rank

    # 1) Split join paths into its pairs, then 2) sort each pair individually, then 3) assemble again

    num_jumps = sorted([len(x) for x in materializable_join_paths])[-1]
    jump_joins = {i: [] for i in range(num_jumps)}

    # 1) split
    for annotated_jp in materializable_join_paths:
        for i, jp in enumerate(annotated_jp):
            jump_joins[i].append(jp)

    def field_to_rank(table, field):
        return table_field_rank[table][field]

    # 2) sort
    for jump, joins in jump_joins.items():
        joins = sorted(joins, key=lambda x: field_to_rank(x[1].source_name, x[1].field_name))
        jump_joins[jump] = joins

    # 3) assemble
    ranked_materialized_join_paths = [[] for _ in range(len(materializable_join_paths))]
    for jump, joins in jump_joins.items():
        for i, join in enumerate(joins):
            ranked_materialized_join_paths[i].append(join)

    return ranked_materialized_join_paths


def obtain_table_paths(set_nids, dod):
    table_path = dict()
    for table, nid in set_nids.items():
        path = dod.aurum_api.helper.get_path_nid(nid)
        table_path[table] = path
    return table_path


def test_e2e(dod, number_jps=5):

    # attrs = ["Mit Id", "Krb Name", "Hr Org Unit Title"]
    # values = ["968548423", "kimball", "Mechanical Engineering"]

    # # cannot search for numbers
    # attrs = ["s_name", "s_address", "ps_availqty"]
    # values = ["Supplier#000000001", "N kD4on9OM Ipw3,gf0JBoQDd7tgrzrddZ", "7340"]

    # attrs = ["s_name", "s_address", "ps_comment"]
    # values = ["Supplier#000000001", "N kD4on9OM Ipw3,gf0JBoQDd7tgrzrddZ",
    #           "dly final packages haggle blithely according to the pending packages. slyly regula"]

    attrs = ["n_name", "s_name", "c_name", "o_clerk"]
    values = ["CANADA", "Supplier#000000013", "Customer#000000005", "Clerk#000000400"]

    # attrs = ["Subject", "Title", "Publisher"]
    # values = ["", "Man who would be king and other stories", "Oxford university press, incorporated"]

    # attrs = ["Iap Category Name", "Person Name", "Person Email"]
    # # values = ["", "Meghan Kenney", "mkenney@mit.edu"]
    # values = ["Engineering", "", ""]

    # attrs = ["Building Name Long", "Ext Gross Area", "Building Room", "Room Square Footage"]
    # values = ["", "", "", ""]

    # attrs = ["c_name", "c_phone", "n_name", "l_tax"]
    # values = ["Customer#000000001", "25-989-741-2988", "BRAZIL", ""]

    # attrs = ["Last Name", "Building Name", "Bldg Gross Square Footage", "Department Name"]
    # values = ["Madden", "Ray and Maria Stata Center", "", "Dept of Electrical Engineering & Computer Science"]

    i = 0
    for mjp, attrs_project in dod.virtual_schema_iterative_search(attrs, values, debug_enumerate_all_jps=False):
        print("JP: " + str(i))
        # i += 1
        # print(mjp.head(2))
        # if i > number_jps:
        #     break

        proj_view = dpu.project(mjp, attrs_project)

        print(str(proj_view.head(10)))

        print("")
        input("Press any key to continue...")


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


def test_intree(dod):
    for mjp, attrs in test_dpu(dod):
        print(mjp.head(2))


def test_dpu(dod):
    with open("check_debug.pkl", 'rb') as f:
        clean_jp = pickle.load(f)

    for mjp in clean_jp:
        attrs_to_project = dpu.obtain_attributes_to_project(mjp)
        # materialized_virtual_schema = dpu.materialize_join_path(mjp, self)
        materialized_virtual_schema = dpu.materialize_join_graph(mjp, dod)
        yield materialized_virtual_schema, attrs_to_project


if __name__ == "__main__":
    print("DoD")

    from knowledgerepr import fieldnetwork
    from modelstore.elasticstore import StoreHandler
    # basic test
    path_to_serialized_model = "/Users/ra-mit/development/discovery_proto/models/tpch/"
    # path_to_serialized_model = "/Users/ra-mit/development/discovery_proto/models/newmitdwh/"
    # path_to_serialized_model = "/Users/ra-mit/development/discovery_proto/models/debug_sb_bug/"
    # sep = ","
    sep = "|"
    store_client = StoreHandler()
    network = fieldnetwork.deserialize_network(path_to_serialized_model)

    dod = DoD(network=network, store_client=store_client, csv_separator=sep)

    test_e2e(dod, number_jps=10)

    # debug intree mat join
    # test_intree(dod)

    # test_joinable(dod)
