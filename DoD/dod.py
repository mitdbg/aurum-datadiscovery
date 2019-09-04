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
from knowledgerepr import fieldnetwork
from modelstore.elasticstore import StoreHandler
import time
from DoD import view_4c_analysis_baseline as v4c
import os
import pandas as pd
import pprint


pp = pprint.PrettyPrinter(indent=4)


class DoD:

    def __init__(self, network, store_client, csv_separator=","):
        self.aurum_api = API(network=network, store_client=store_client)
        self.paths_cache = dict()
        dpu.configure_csv_separator(csv_separator)

    def place_paths_in_cache(self, t1, t2, paths):
        self.paths_cache[(t1, t2)] = paths
        self.paths_cache[(t2, t1)] = paths

    def are_paths_in_cache(self, t1, t2):
        if (t1, t2) in self.paths_cache:
            print("HIT!")
            return self.paths_cache[(t1, t2)]
        elif (t2, t1) in self.paths_cache:
            print("HIT!")
            return self.paths_cache[(t2, t1)]
        else:
            return None

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

    def virtual_schema_iterative_search(self, list_attributes: [str], list_samples: [str], perf_stats, max_hops=2, debug_enumerate_all_jps=False):
        # Align schema definition and samples
        st_stage1 = time.time()
        assert len(list_attributes) == len(list_samples)
        sch_def = {attr: value for attr, value in zip(list_attributes, list_samples)}

        sch_def = OrderedDict(sorted(sch_def.items(), key=lambda x: x[0], reverse=True))

        filter_drs = self.joint_filters(sch_def)
        et_stage1 = time.time()
        perf_stats['t_stage1'] = (et_stage1 - st_stage1)
        st_stage2 = time.time()
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
            backup = []
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
                    if covers_filters(candidate_group_filters_covered, filter_drs.items()):
                        yield (candidate_group, candidate_group_filters_covered)
                    else:
                        backup.append(([el for el in candidate_group],
                                       set([el for el in candidate_group_filters_covered])))
                    # Cleaning
                    clear_state()
                # before exiting, return backup in case that may be useful
                for candidate_group, candidate_group_filters_covered in backup:
                    yield (candidate_group, candidate_group_filters_covered)
                go_on = False  # finished exploring all groups

        # """
        # # FIXME: obtaining pairs of tables to join?
        # """
        # all_pairs = 0
        # candidate_groups = 0
        # for cg, _ in eager_candidate_exploration():
        #     candidate_groups += 1
        #     all_pairs += len([el for el in list(itertools.combinations(cg, 2))])
        #     # all_pairs_to_join = [len([el for el in list(itertools.combinations(group_tables, 2))])
        #     #                  for group_tables in all_candidate_groups]
        #     # all_pairs += all_pairs_to_join[0]
        # # print([el for el in all_candidate_groups])
        # # print("all pairs to join: " + str(all_pairs))
        # print("CG: " + str(candidate_groups))
        # print("TOTAL: " + str(all_pairs))
        # exit()
        # """
        # # FIXME
        # """

        et_stage2 = time.time()
        perf_stats['t_stage2'] = (et_stage2 - st_stage2)
        # Find ways of joining together each group
        cache_unjoinable_pairs = defaultdict(int)
        perf_stats['time_joinable'] = 0
        perf_stats['time_is_materializable'] = 0
        perf_stats['time_materialize'] = 0
        num_candidate_groups = 0
        for candidate_group, candidate_group_filters_covered in eager_candidate_exploration():
            num_candidate_groups += 1
            print("")
            print("Candidate group: " + str(candidate_group))
            num_unique_filters = len({f_id for _, _, f_id in candidate_group_filters_covered})
            print("Covers #Filters: " + str(num_unique_filters))

            if len(candidate_group) == 1:
                table = candidate_group[0]
                path = table_path[table]
                # materialized_virtual_schema = dpu.get_dataframe(path + "/" + table)
                materialized_virtual_schema = dpu.read_relation(path + "/" + table)
                attrs_to_project = dpu.obtain_attributes_to_project(candidate_group_filters_covered)
                # Create metadata to document this view
                view_metadata = dict()
                view_metadata["#join_graphs"] = 1
                view_metadata["join_graph"] = {"nodes": [{"id": -101010, "label": table}], "edges": []}
                if 'single_relation_group' not in perf_stats:
                    perf_stats['single_relation_group'] = 0
                perf_stats['single_relation_group'] += 1
                yield materialized_virtual_schema, attrs_to_project, view_metadata
                continue  # to go to the next group

            # Pre-check
            # TODO: with a connected components index we can pre-filter many of those groups without checking
            #group_with_all_relations, join_path_groups = self.joinable(candidate_group, cache_unjoinable_pairs)
            max_hops = max_hops
            # We find the different join graphs that would join the candidate_group
            st_joinable = time.time()
            join_graphs = self.joinable(candidate_group, cache_unjoinable_pairs, max_hops=max_hops)
            et_joinable = time.time()
            perf_stats['time_joinable'] += (et_joinable - st_joinable)
            if debug_enumerate_all_jps:
                for i, group in enumerate(join_graphs):
                    print("Group: " + str(i))
                    for el in group:
                        print(el)
                continue  # We are just interested in all JPs for all candidate groups

            # if not graphs skip next
            if len(join_graphs) == 0:
                if 'unjoinable_candidate_group' not in perf_stats:
                    perf_stats['unjoinable_candidate_group'] = 0
                perf_stats['unjoinable_candidate_group'] += 1
                print("Group: " + str(candidate_group) + " is Non-Joinable with max_hops=" + str(max_hops))
                continue
            if 'joinable_candidate_group' not in perf_stats:
                perf_stats['joinable_candidate_group'] = 0
            perf_stats['joinable_candidate_group'] += 1
            if 'num_join_graphs_per_candidate_group' not in perf_stats:
                perf_stats['num_join_graphs_per_candidate_group'] = []
            perf_stats['num_join_graphs_per_candidate_group'].append(len(join_graphs))

            # Now we need to check every join graph individually and see if it's materializable. Only once we've
            # exhausted these join graphs we move on to the next candidate group. We know already that each of the
            # join graphs covers all tables in candidate_group, so if they're materializable we're good.
            total_materializable_join_graphs = 0
            materializable_join_graphs = []
            for jpg in join_graphs:
                # Obtain filters that apply to this join graph
                filters = set()
                for l, r in jpg:
                    if l.source_name in table_fulfilled_filters:
                        filters.update(table_fulfilled_filters[l.source_name])
                    if r.source_name in table_fulfilled_filters:
                        filters.update(table_fulfilled_filters[r.source_name])

                # TODO: obtain join_graph score for diff metrics. useful for ranking later
                # rank_materializable_join_graphs(materializable_join_paths, table_path, dod)
                st_is_materializable = time.time()
                # if query view is all attributes, then it's always materializable or we could
                # join on a small sample and see -- we can have 2 different impls.
                if sum([0] + [1 for el in list_samples if el != '']) > 0:
                    is_join_graph_valid = self.is_join_graph_materializable(jpg, table_fulfilled_filters)
                else:
                    is_join_graph_valid = True
                et_is_materializable = time.time()
                perf_stats['time_is_materializable'] += (et_is_materializable - st_is_materializable)
                # Obtain all materializable graphs, then materialize
                if is_join_graph_valid:
                    total_materializable_join_graphs += 1
                    materializable_join_graphs.append((jpg, filters))
            # At this point we can empty is-join-graph-materializable cache and create a new one
            # dpu.empty_relation_cache()  # TODO: If df.copy() works, then this is a nice reuse
            st_materialize = time.time()
            to_return = self.materialize_join_graphs(materializable_join_graphs)
            et_materialize = time.time()
            perf_stats['time_materialize'] += (et_materialize - st_materialize)
            # yield to_return
            for el in to_return:
                if 'actually_materialized' not in perf_stats:
                    perf_stats['actually_materialized'] = 0
                perf_stats['actually_materialized'] += 1
                yield el

            if 'materializable_join_graphs' not in perf_stats:
                perf_stats['materializable_join_graphs'] = []
            perf_stats['materializable_join_graphs'].append(total_materializable_join_graphs)

        perf_stats["num_candidate_groups"] = num_candidate_groups
        print("Finished enumerating groups")
        cache_unjoinable_pairs = OrderedDict(sorted(cache_unjoinable_pairs.items(),
                                                    key=lambda x: x[1], reverse=True))
        for k, v in cache_unjoinable_pairs.items():
            print(str(k) + " => " + str(v))

    def compute_join_graph_id(self, join_graph):
        all_nids = []
        for hop_l, hop_r in join_graph:
            all_nids.append(hop_r.nid)
            all_nids.append(hop_l.nid)
        path_id = sum([hash(el) for el in all_nids])
        return path_id

    def __compute_join_graph_id(self, join_graph):
        all_nids = []
        for hop_l, hop_r in join_graph:
            all_nids.append(hop_r.nid)
            all_nids.append(hop_l.nid)
        path_id = frozenset(all_nids)
        return path_id

    def materialize_join_graphs(self, materializable_join_graphs):
        to_return = []
        for mjg, filters in materializable_join_graphs:
            # if is_join_graph_valid:
            attrs_to_project = dpu.obtain_attributes_to_project(filters)
            # continue  # test
            materialized_virtual_schema = dpu.materialize_join_graph_sample(mjg, self, sample_size=1000)
            # materialized_virtual_schema = dpu.materialize_join_graph(mjg, self)
            if materialized_virtual_schema is False:
                continue  # happens when the join was an outlier
            # Create metadata to document this view
            view_metadata = dict()
            view_metadata["#join_graphs"] = len(materializable_join_graphs)
            # view_metadata["join_graph"] = self.format_join_paths_pairhops(jpg)
            view_metadata["join_graph"] = self.format_join_graph_into_nodes_edges(mjg)
            to_return.append((materialized_virtual_schema, attrs_to_project, view_metadata))
            # yield materialized_virtual_schema, attrs_to_project, view_metadata
        return to_return

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

        table_combinations = [el for el in itertools.combinations(group_tables, 2)]

        for table1, table2 in tqdm(table_combinations):
            # Check if tables are already known to be unjoinable
            if (table1, table2) in cache_unjoinable_pairs.keys() or (table2, table1) in cache_unjoinable_pairs.keys():
                continue  # FIXME FIXME FIXME
            t1 = self.aurum_api.make_drs(table1)
            t2 = self.aurum_api.make_drs(table2)
            t1.set_table_mode()
            t2.set_table_mode()
            # Check cache first, if not in cache then do the search
            # drs = self.are_paths_in_cache(table1, table2)
            paths = self.are_paths_in_cache(table1, table2)  # list of lists
            if paths is None:
                print("Finding paths between " + str(table1) + " and " + str(table2))
                print("max hops: " + str(max_hops))
                s = time.time()
                drs = self.aurum_api.paths(t1, t2, Relation.PKFK, max_hops=max_hops, lean_search=True)
                e = time.time()
                print("Total time: " + str((e-s)))
                paths = drs.paths()  # list of lists
                self.place_paths_in_cache(table1, table2, paths)  # FIXME FIXME FIXME
            # paths = drs.paths()  # list of lists
            # If we didn't find paths, update unjoinable_pairs cache with this pair
            if len(paths) == 0:  # then store this info, these tables do not join # FIXME FIXME FIXME
                cache_unjoinable_pairs[(table1, table2)] += 1  # FIXME FIXME
                cache_unjoinable_pairs[(table2, table1)] += 1  # FIXME FIXME FIXME
            for p in paths:
                tables_covered = set()
                tables_in_group = set(group_tables)
                for hop in p:
                    if hop.source_name in tables_in_group:
                        # this is a table covered by this path
                        tables_covered.add(hop.source_name)
                paths_per_pair[(table1, table2)].append((p, tables_covered))

        if len(paths_per_pair) == 0:
            return []

        # enumerate all possible join graphs
        all_combinations = [el for el in itertools.product(*list(paths_per_pair.values()))]
        deduplicated_paths = dict()
        # Add combinations
        for path_combination in all_combinations:
            # TODO: is this general if max_hops > 2?
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
                    # Transform paths into pair-hops so we can join them together
                    tx_path1 = self.transform_join_path_to_pair_hop(path1)
                    tx_path2 = self.transform_join_path_to_pair_hop(path2)
                    # combine the paths
                    combined_path = tx_path1 + tx_path2
                    path_id = self.compute_join_graph_id(combined_path)
                    # If I haven't generated this path elsewhere, then I add it along with the tables it covers
                    if path_id not in deduplicated_paths:
                        deduplicated_paths[path_id] = (combined_path, potential_cover)

        # Add initial paths that may cover all tables and remove those that do not
        for p, tables_covered in list(paths_per_pair.values())[0]:
            if len(tables_covered) == len(group_tables):
                tx_p = self.transform_join_path_to_pair_hop(p)
                path_id = self.compute_join_graph_id(tx_p)
                deduplicated_paths[path_id] = (tx_p, tables_covered)

        # Now we filter out all paths that do not cover all the tables in the group
        covering_join_graphs = [jg[0] for _, jg in deduplicated_paths.items() if len(jg[1]) == len(group_tables)]

        # Finally sort by number of required joins
        join_graphs = sorted(covering_join_graphs, key=lambda x: len(x))
        return join_graphs

    def format_join_graph_into_nodes_edges(self, join_graph):
        nodes = dict()
        edges = []
        for jp in join_graph:
            # Add nodes
            for hop in jp:
                label = hop.db_name + ":" + hop.source_name
                node_descr = {"id": hash(label), "label": label}  # cannot use nid cause that's for cols not rels
                node_id = hash(label)
                if node_id not in nodes:
                    nodes[node_id] = node_descr
            l, r = jp
            l_label = l.db_name + ":" + l.source_name
            r_label = r.db_name + ":" + r.source_name
            edge_descr = {"from": hash(l_label), "to": hash(r_label)}
            edges.append(edge_descr)
        return {"nodes": list(nodes.values()), "edges": list(edges)}

    def transform_join_path_to_pair_hop(self, join_path):
        """
        1. get join path, 2. annotate with values to check for, then 3. format into [(l,r)]
        :param join_paths:
        :param table_fulfilled_filters:
        :return:
        """
        jp_hops = []
        pair = []
        for hop in join_path:
            pair.append(hop)
            if len(pair) == 2:
                jp_hops.append(tuple(pair))
                pair.clear()
                pair.append(hop)
        # Now remove pairs with pointers within same relation, as we don't need to join these
        jp_hops = [(l, r) for l, r in jp_hops if l.source_name != r.source_name]
        return jp_hops

    def is_join_graph_materializable(self, join_graph, table_fulfilled_filters):
        # FIXME: add a way of collecting the join cardinalities and propagating them outside as well
        local_intermediates = dict()

        for l, r in join_graph:
            # apply filters to l
            if l.source_name not in local_intermediates:
                l_path = self.aurum_api.helper.get_path_nid(l.nid)
                # If there are filters apply them
                if l.source_name in table_fulfilled_filters:
                    filters_l = table_fulfilled_filters[l.source_name]
                    filtered_l = None
                    for info, filter_type, filter_id in filters_l:
                        if filter_type == FilterType.ATTR:
                            filtered_l = dpu.read_relation_on_copy(l_path + l.source_name)  # FIXME FIXME FIXME
                            # filtered_l = dpu.get_dataframe(l_path + l.source_name)
                            continue  # no need to filter anything if the filter is only attribute type
                        attribute = info[1]
                        cell_value = info[0]
                        filtered_l = dpu.apply_filter(l_path + l.source_name, attribute, cell_value)# FIXME FIXME FIXME

                    if len(filtered_l) == 0:
                        return False  # filter does not leave any data => non-joinable
                # If there are not filters, then do not apply them
                else:
                    filtered_l = dpu.read_relation_on_copy(l_path + l.source_name)# FIXME FIXME FIXME
                    # filtered_l = dpu.get_dataframe(l_path + l.source_name)
            else:
                filtered_l = local_intermediates[l.source_name]
            local_intermediates[l.source_name] = filtered_l

            # apply filters to r
            if r.source_name not in local_intermediates:
                r_path = self.aurum_api.helper.get_path_nid(r.nid)
                # If there are filters apply them
                if r.source_name in table_fulfilled_filters:
                    filters_r = table_fulfilled_filters[r.source_name]
                    filtered_r = None
                    for info, filter_type, filter_id in filters_r:
                        if filter_type == FilterType.ATTR:
                            filtered_r = dpu.read_relation_on_copy(r_path + r.source_name)# FIXME FIXME FIXME
                            # filtered_r = dpu.get_dataframe(r_path + r.source_name)
                            continue  # no need to filter anything if the filter is only attribute type
                        attribute = info[1]
                        cell_value = info[0]
                        filtered_r = dpu.apply_filter(r_path + r.source_name, attribute, cell_value)

                    if len(filtered_r) == 0:
                        return False  # filter does not leave any data => non-joinable
                # If there are not filters, then do not apply them
                else:
                    filtered_r = dpu.read_relation_on_copy(r_path + r.source_name)# FIXME FIXME FIXME
                    # filtered_r = dpu.get_dataframe(r_path + r.source_name)
            else:
                filtered_r = local_intermediates[r.source_name]
            local_intermediates[r.source_name] = filtered_r

            # check if the materialized version join's cardinality > 0
            joined = dpu.join_ab_on_key(filtered_l, filtered_r, l.field_name, r.field_name, suffix_str="_x")

            if len(joined) == 0:
                return False  # non-joinable hop enough to discard join graph
        # if we make it through all hopes, then join graph is materializable (i.e., verified)
        return True


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


def test_e2e(dod, attrs, values, number_jps=5, output_path=None, full_view=False, interactive=False):

    ###
    # Run Core DoD
    ###
    view_metadata_mapping = dict()
    i = 0
    perf_stats = dict()
    st_runtime = time.time()
    for mjp, attrs_project, metadata in dod.virtual_schema_iterative_search(attrs, values, perf_stats, max_hops=2,
                                                        debug_enumerate_all_jps=False):
        print("JP: " + str(i))
        # i += 1
        # print(mjp.head(2))
        # if i > number_jps:
        #     break

        proj_view = dpu.project(mjp, attrs_project)

        # print(str(proj_view.head(5)))
        # print("Metadata")
        # print(metadata)

        if output_path is not None:
            view_path = None
            if full_view:
                view_path = output_path + "/raw_view_" + str(i)
                mjp.to_csv(view_path, encoding='latin1', index=False)
            view_path = output_path + "/view_" + str(i)
            proj_view.to_csv(view_path, encoding='latin1', index=False)  # always store this
            # store metadata associated to that view
            view_metadata_mapping[view_path] = metadata

        i += 1

        if interactive:
            print("")
            input("Press any key to continue...")
    et_runtime = time.time()
    perf_stats['runtime'] = (et_runtime - st_runtime)
    pp.pprint(perf_stats)
    if 'num_join_graphs_per_candidate_group' in perf_stats:
        total_join_graphs = sum(perf_stats['num_join_graphs_per_candidate_group'])
        print("Total join graphs: " + str(total_join_graphs))
    if 'materializable_join_graphs' in perf_stats:
        total_materializable_join_graphs = sum(perf_stats['materializable_join_graphs'])
        print("Total materializable join graphs: " + str(total_materializable_join_graphs))

    print("Total views: " + str(i))
    exit()

    ###
    # Run 4C
    ###
    # return
    groups_per_column_cardinality = v4c.main(output_path)

    for k, v in groups_per_column_cardinality.items():
        compatible_groups = v['compatible']
        contained_groups = v['contained']
        complementary_group = v['complementary']
        contradictory_group = v['contradictory']

        print("Compatible views: " + str(len(compatible_groups)))
        print("Contained views: " + str(len(contained_groups)))
        print("Complementary views: " + str(len(complementary_group)))
        print("Contradictory views: " + str(len(contradictory_group)))

        # for path1, key_column, key_value, path2 in contradictory_group[:2]:
        #     df1 = pd.read_csv(path1)
        #     df2 = pd.read_csv(path2)
        #     row1 = df1[df1[key_column] == key_value]
        #     row2 = df2[df2[key_column] == key_value]
        #     print(path1 + " - " + path2)
        #     print("ROW - 1")
        #     print(view_metadata_mapping[path1])
        #     print(row1)
        #     print("ROW - 2")
        #     print(view_metadata_mapping[path2])
        #     print(row2)
        #     print("")
        #     print("")

    # We can now link classified views with their metadata



# def test_joinable(dod):
#     candidate_group = ['Employee_directory.csv', 'Drupal_employee_directory.csv']
#     #candidate_group = ['Se_person.csv', 'Employee_directory.csv', 'Drupal_employee_directory.csv']
#     #candidate_group = ['Tip_detail.csv', 'Tip_material.csv']
#     join_paths = dod.joinable(candidate_group)
#
#     # print("RAW: " + str(len(join_paths)))
#     # for el in join_paths:
#     #     print(el)
#
#     join_paths = dod.format_join_paths_pairhops(join_paths)
#
#     print("CLEAN: " + str(len(join_paths)))
#     for el in join_paths:
#         print(el)


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


def main(args):
    model_path = args.model_path
    separator = args.separator

    store_client = StoreHandler()
    network = fieldnetwork.deserialize_network(model_path)
    dod = DoD(network=network, store_client=store_client, csv_separator=separator)

    attrs = args.list_attributes.split(";")
    values = args.list_values.split(";")
    print(attrs)
    print(values)
    assert len(attrs) == len(values)

    i = 0
    for mjp, attrs_project, metadata in dod.virtual_schema_iterative_search(attrs, values,
                                                                            debug_enumerate_all_jps=False):
        print("JP: " + str(i))
        proj_view = dpu.project(mjp, attrs_project)
        print(str(proj_view.head(10)))
        print("Metadata")
        print(metadata)
        if args.output_path:
            if args.full_view:
                mjp.to_csv(args.output_path + "/raw_view_" + str(i), encoding='latin1', index=False)
            proj_view.to_csv(args.output_path + "/view_" + str(i), encoding='latin1', index=False)  # always store this
        i += 1
        if args.interactive == "True":
            print("")
            input("Press any key to continue...")


def pe_paths(dod):
    s = time.time()
    table1 = "Fclt_building_list.csv"
    table2 = "short_course_catalog_subject_offered.csv"
    # table1 = "Warehouse_users.csv"
    # table2 = "short_course_catalog_subject_offered.csv"
    # table1 = "Fclt_building_list.csv"
    # table2 = "Se_person.csv"
    t1 = dod.aurum_api.make_drs(table1)
    t2 = dod.aurum_api.make_drs(table2)
    t1.set_table_mode()
    t2.set_table_mode()
    i = time.time()
    drs = dod.aurum_api.paths(t1, t2, Relation.PKFK, max_hops=2, lean_search=True)
    a = drs.paths()
    e = time.time()
    print("Total time: " + str((e - s)))
    print("Inter time: " + str((i - s)))
    print("Done")


if __name__ == "__main__":
    print("DoD")

    ###
    ## Setup DoD
    ###
    # path_to_serialized_model = "/Users/ra-mit/development/discovery_proto/models/tpch/"
    # path_to_serialized_model = "/Users/ra-mit/development/discovery_proto/models/mitdwh/"
    # path_to_serialized_model = "/Users/ra-mit/development/discovery_proto/models/debug_sb_bug/"
    # path_to_serialized_model = "/Users/ra-mit/development/discovery_proto/models/massdata/"
    path_to_serialized_model = "/Users/ra-mit/development/discovery_proto/models/chembl_and_drugcentral/"
    # sep = ","
    # sep = "|"
    sep = ";"
    store_client = StoreHandler()
    network = fieldnetwork.deserialize_network(path_to_serialized_model)
    dod = DoD(network=network, store_client=store_client, csv_separator=sep)

    ###
    ## Query Views
    ###

    ### TPCH

    # # cannot search for numbers
    # attrs = ["s_name", "s_address", "ps_availqty"]
    # values = ["Supplier#000000001", "N kD4on9OM Ipw3,gf0JBoQDd7tgrzrddZ", "7340"]

    # attrs = ["s_name", "s_address", "ps_comment"]
    # values = ["Supplier#000000001", "N kD4on9OM Ipw3,gf0JBoQDd7tgrzrddZ",
    #           "dly final packages haggle blithely according to the pending packages. slyly regula"]

    # attrs = ["n_name", "s_name", "c_name", "o_clerk"]
    # values = ["CANADA", "Supplier#000000013", "Customer#000000005", "Clerk#000000400"]

    # attrs = ["o_clerk", "o_orderpriority", "n_name"]
    # values = ["Clerk#000000951", "5-LOW", "JAPAN"]

    # attrs = ["c_name", "c_phone", "n_name", "l_tax"]
    # values = ["Customer#000000001", "25-989-741-2988", "BRAZIL", ""]

    ## MIT DWH

    # tests equivalence and containment - did not finish executing though (out of memory)
    # attrs = ["Mit Id", "Krb Name", "Hr Org Unit Title"]
    # values = ["968548423", "kimball", "Mechanical Engineering"]

    # attrs = ["Subject", "Title", "Publisher"]
    # values = ["", "Man who would be king and other stories", "Oxford university press, incorporated"]

    # EVAL - ONE
    # attrs = ["Iap Category Name", "Person Name", "Person Email"]
    # # values = ["", "Meghan Kenney", "mkenney@mit.edu"]
    # values = ["Engineering", "", ""]

    # EVAL - TWO
    # attrs = ["Building Name Long", "Ext Gross Area", "Building Room", "Room Square Footage"]
    # values = ["", "", "", ""]

    # EVAL - THREE
    # attrs = ["Last Name", "Building Name", "Bldg Gross Square Footage", "Department Name"]
    # values = ["Madden", "Ray and Maria Stata Center", "", "Dept of Electrical Engineering & Computer Science"]

    # EVAL - FOUR
    # tests equivalence and containment
    # attrs = ["Email Address", "Department Full Name"]
    # values = ["madden@csail.mit.edu", ""]

    # EVAL - FIVE
    # attrs = ["Last Name", "Building Name", "Bldg Gross Square Footage", "Department Name"]
    # values = ["", "", "", ""]

    ## MASSDATA

    # ONE (3 + 4)
    # attrs = ["Neighborhood ", "Total Population ", "Graduate Degree %"]
    # values = ["Cambridgeport", "", ""]

    #
    # attrs = ['CASE_TITLE', 'SUBJECT', 'Graduate Degree %']
    # values = ['', 'Public Works Department', '']

    ## CHEMBL22

    # ONE (12)
    attrs = ['assay_test_type', 'assay_category', 'journal', 'year', 'volume']
    values = ['', '', '', '', '']

    # TWO (27)
    # attrs = ['accession', 'sequence', 'organism', 'start_position', 'end_position']
    # values = ['O09028', '', 'Rattus norvegicus', '', '']

    # THREE (50)
    # attrs = ['ref_type', 'ref_url', 'enzyme_name', 'organism']
    # values = ['', '', '', '']

    # FOUR (54)
    # attrs = ['hba', 'hbd', 'parenteral', 'topical']
    # values = ['', '', '', '']

    # FIVE (100-)
    # attrs = ['accession', 'sequence', 'organism', 'start_position', 'end_position']
    # values = ['', '', '', '', '']

    output_path = "/Users/ra-mit/development/discovery_proto/data/dod/test/"

    # remove all files in test
    for f in os.listdir(output_path):
        f_path = os.path.join(output_path, f)
        try:
            if os.path.isfile(f_path):
                os.unlink(f_path)
        except Exception as e:
            print(e)

    # test_e2e(dod, number_jps=10, output_path=None, interactive=False)
    # output_path = None
    test_e2e(dod, attrs, values, number_jps=10, output_path=output_path)

    # debug intree mat join
    # test_intree(dod)

    # test_joinable(dod)

