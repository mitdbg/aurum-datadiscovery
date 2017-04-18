from enum import Enum
import time
from collections import defaultdict
from nltk.corpus import stopwords
from dataanalysis import nlp_utils as nlp
from ontomatch import glove_api
from ontomatch import ss_utils as SS
from datasketch import MinHash, MinHashLSH
from knowledgerepr.networkbuilder import LSHRandomProjectionsIndex
from dataanalysis import dataanalysis as da
import operator
from collections import namedtuple


class MatchingType(Enum):
    L1_CLASSNAME_ATTRVALUE = 0
    L2_CLASSVALUE_ATTRVALUE = 1
    L3_CLASSCTX_RELATIONCTX = 2
    L4_CLASSNAME_RELATIONNAME_SYN = 3
    L42_CLASSNAME_RELATIONNAME_SEM = 4
    L5_CLASSNAME_ATTRNAME_SYN = 5
    L52_CLASSNAME_ATTRNAME_SEM = 6
    L6_CLASSNAME_RELATION_SEMSIG = 7
    L7_CLASSNAME_ATTRNAME_FUZZY = 8


Leave = namedtuple('Leave', 'id, matching')


class SimpleTrie:

    def __init__(self):
        self._leave = 0
        self.root = dict()
        self.step_dic = defaultdict(int)
        self.summarized_matchings = dict()

    def add_sequences(self, sequences, seq_corresponding_matching):
        self.step_dic["root"] = len(sequences)  # add also the number of sequences
        for seq in sequences:
            current_dict = self.root
            for token in seq:
                current_dict = current_dict.setdefault(token, {})  # another dict as default
                self.step_dic[token] += 1
            self._leave += 1  # increase leave id
            leave = Leave(self._leave, seq_corresponding_matching[str(seq)])  # create leave and assign matchings
            current_dict[self._leave] = leave
        return self.root, self.step_dic

    def _reduce_matchings(self, subtree, output):
        if type(subtree) is Leave:
            for el in subtree.matching:
                output.add(el)
        else:
            for child in subtree.keys():
                if type(child) is not Leave:
                    output = self._reduce_matchings(subtree[child], output)
                elif type(child) is Leave:
                    for el in subtree[child].matching:
                        output.add(el)
        return output

    def _add_matchings(self, subtree, child):
        subtree = subtree[child]
        if type(subtree) is Leave:
            matchings_of_child = subtree.matching
            for el in matchings_of_child:
                self.summarized_matchings[el] = 1  # the child
            return
        matchings = self._reduce_matchings(subtree, set())
        sch, cla = list(matchings)[0]
        new_match = (sch, (cla[0], child))  # child summarizes all the others
        self.summarized_matchings[new_match] = len(matchings)  # the number

    def _add_matchings2(self, subtree, parent):

        matchings = self._reduce_matchings(subtree, set())
        sch, cla = list(matchings)[0]
        new_match = (sch, (cla[0], parent))  # child summarizes all the others
        self.summarized_matchings[new_match] = len(matchings)  # the number

    """
    def cuts(self, current_node, subtree=None, num_seqs=None):
        if subtree is None and num_seqs is None:
            subtree = self.root
            num_seqs = self.step_dic["root"]

        #children = len(subtree.keys())
        children_represented = self.step_dic[current_node]
        ratio_cut = float(children / num_seqs)
        if ratio_cut > 0.5:
            return True
        return False
    """

    def summarize(self, num_seqs):

        def summarize_seq(num_seqs, subtree=None, current_node=None):

            # Choose the max representing child
            max_repr = 0
            chosen_child = None
            for child in subtree.keys():
                represented_seqs = self.step_dic[child]
                if represented_seqs > max_repr:
                    max_repr = represented_seqs
                    chosen_child = child

            # Does the max representing child cuts?
            ratio_cut = float(max_repr / num_seqs)
            if ratio_cut > 0.4:  # if cuts, keep digging
                return summarize_seq(num_seqs, subtree[chosen_child], chosen_child)
            else:  # i then summarize
                matchings = self._reduce_matchings(subtree, set())
                return matchings, current_node

        matchings, cutter = summarize_seq(num_seqs, self.root, "root")
        #sch, cla = list(matchings)[0]
        #new_match = (sch, (cla[0], cutter))
        return matchings, cutter


class Matching:

    def __init__(self, db_name, source_name):
        self.db_name = db_name
        self.source_name = source_name
        self.source_level_matchings = defaultdict(lambda: defaultdict(list))
        self.attr_matchings = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

    def add_relation_correspondence(self, kr_name, class_name, matching_type):
        self.source_level_matchings[kr_name][class_name].append(matching_type)

    def add_attribute_correspondence(self, attr_name, kr_name, class_name, matching_type):
        self.attr_matchings[attr_name][kr_name][class_name].append(matching_type)

    def __str__(self):
        header = self.db_name + " - " + self.source_name
        relation_matchings = list()
        relation_matchings.append(header)
        if len(self.source_level_matchings.items()) > 0:
            for kr_name, values in self.source_level_matchings.items():
                for class_name, matchings in values.items():
                    line = kr_name + " - " + class_name + " : " + str(matchings)
                    relation_matchings.append(line)
        else:
            line = "0 relation matchings"
            relation_matchings.append(line)
        if len(self.attr_matchings.items()) > 0:
            for attr_name, values in self.attr_matchings.items():
                for kr_name, classes in values.items():
                    for class_name, matchings in classes.items():
                        line = attr_name + " ==>> " + kr_name + " - " + class_name + " : " + str(matchings)
                        relation_matchings.append(line)
        string_repr = '\n'.join(relation_matchings)
        return string_repr

    def get_matchings(self):
        matchings = []
        for kr_name, values in self.source_level_matchings.items():
            for class_name, ms in values.items():
                match = ((self.db_name, self.source_name, "_"), (kr_name, class_name))
                matchings.append(match)
        for attr_name, values in self.attr_matchings.items():
            for kr_name, classes in values.items():
                for class_name, ms in classes.items():
                    match = ((self.db_name, self.source_name, attr_name), (kr_name, class_name))
                    matchings.append(match)
        return matchings

    def print_serial(self):
        relation_matchings = []
        for kr_name, values in self.source_level_matchings.items():
            for class_name, matchings in values.items():
                line = self.db_name + " %%% " + self.source_name + " %%% _ ==>> " + kr_name \
                       + " %%% " + class_name + " %%% " + str(matchings)
                relation_matchings.append(line)
        for attr_name, values in self.attr_matchings.items():
            for kr_name, classes in values.items():
                for class_name, matchings in classes.items():
                    line = self.db_name + " %%% " + self.source_name + " %%% " + attr_name \
                           + " ==>> " + kr_name + " %%% " + class_name + " %%% " + str(matchings)
                    relation_matchings.append(line)
        #string_repr = '\n'.join(relation_matchings)
        return relation_matchings



# double check for better recall
def double_check_sem_signal_attr_sch_sch(attribute1, attribute2,
                                          penalize_unknown_word=True,
                                          add_exact_matches=True):
    # st = time.time()
    svs1 = []
    field_name1 = attribute1
    field_name1 = nlp.camelcase_to_snakecase(field_name1)
    field_name1 = field_name1.lower()
    field_name1 = field_name1.replace('_', ' ')
    for token in field_name1.split():
        if token not in stopwords.words('english'):
            sv = glove_api.get_embedding_for_word(token)
            if sv is not None:
                svs1.append(sv)
    svs2 = []
    field_name2 = attribute2
    field_name2 = nlp.camelcase_to_snakecase(field_name2)
    field_name2 = field_name2.lower()
    field_name2 = field_name2.replace('_', ' ')
    for token in field_name2.split():
        if token not in stopwords.words('english'):
            sv2 = glove_api.get_embedding_for_word(token)
            if sv2 is not None:
                svs2.append(sv2)

    neg_matchings = []
    ban_index1, ban_index2 = get_ban_indexes(field_name1, field_name2)
    svs_rel = removed_banned_vectors(ban_index1, svs1)
    svs_cla = removed_banned_vectors(ban_index2, svs2)
    semantic_sim, neg_signal = SS.compute_semantic_similarity(svs_rel, svs_cla,
                                                              penalize_unknown_word=penalize_unknown_word,
                                                              add_exact_matches=add_exact_matches)
    # et = time.time()
    # print("negative_sem_signal: " + str(et - st))
    # return neg_matchings

    return semantic_sim, neg_signal


def summarize_matchings_to_ancestor(om, matchings, threshold_to_summarize=2, summarize_or_remove=True, summary_ratio=0.8):

    def get_sem_similar_matchings_from(matchings):
        matchings_to_keep = []
        for el in matchings:
             # double check exact matching only
             # if (el[0][2].lower() == el[1][1].lower()):
             #    matchings_to_keep.append(el)
            # double check using the semantic
            attribute1 = el[0][2]
            attribute2 = el[1][1]
            semantic_sim, neg_signal = double_check_sem_signal_attr_sch_sch(attribute1, attribute2)
            if not neg_signal and semantic_sim > 0.8:
                matchings_to_keep.append(el)
        return matchings_to_keep

    def summarize(matchings, handler):
        sequences = list()
        seq_corresponding_matching = defaultdict(list)
        for el in matchings:
            sch, cla = el
            class_name = cla[1]
            onto_name = cla[0]
            if handler is None:
                handler = om.kr_handlers[onto_name]
            root_to_class_name = handler.ancestors_of_class(class_name)
            root_to_class_name = handler.name_of_sequence(root_to_class_name)
            seq_corresponding_matching[str(root_to_class_name)].append(el)
            sequences.append(root_to_class_name)

        trie = SimpleTrie()
        trie.add_sequences(sequences, seq_corresponding_matching)
        matching_to_be_summarized, cutter = trie.summarize(len(sequences))
        summ_matchings = []
        if (len(matching_to_be_summarized) / len(matchings)) > summary_ratio:  # good summarization
            # get level of class
            root_to_class_name = handler.ancestors_of_class(cutter)
            root_to_class_name = handler.name_of_sequence(root_to_class_name)
            if len(root_to_class_name) > 2:
                sch, cla = list(matching_to_be_summarized)[0]
                new_match = (sch, (cla[0], cutter))  # the match that summarizes the previous
                if summarize_or_remove:
                    summ_matchings.append(new_match)
                    semantically_similar_matchings = get_sem_similar_matchings_from(matchings)
                    for el in semantically_similar_matchings:
                        summ_matchings.append(el)
                    return summ_matchings
                else:
                    summ_matchings = [m for m in matchings if m not in set(matching_to_be_summarized)]
                    summ_matchings.append(new_match)
                    return summ_matchings

        if summarize_or_remove:
            sch, cla = list(matching_to_be_summarized)[0]
            new_match = (sch, (cla[0], cutter))  # don't add -> breaking precision...
            # return []  # could not summarize -> remove
            semantically_similar_matchings = get_sem_similar_matchings_from(matchings)
            return semantically_similar_matchings  # could not summarize -> remove
        else:
            summ_matchings = [m for m in matchings if m not in set(matching_to_be_summarized)]
            sch, cla = list(matching_to_be_summarized)[0]
            new_match = (sch, (cla[0], cutter))  # the match that summarizes the previous
            summ_matchings.append(new_match)
            semantically_similar_matchings = get_sem_similar_matchings_from(matchings)
            for el in semantically_similar_matchings:
                summ_matchings.append(el)
            return summ_matchings  # could not summarize, return original

    def compute_fanout(matchings):
        fanout = defaultdict(lambda: defaultdict(list))
        for m in matchings:
            sch, cla = m
            onto_name = cla[0]
            fanout[sch][onto_name].append(m)
        ordered = sorted(fanout.items(), key=lambda x: len(x[1].values()), reverse=True)
        ordered_dict = dict()
        for key, value in ordered:
            ordered_dict[key] = value
        return ordered_dict

    handler = None  # the handler for the ontology
    summarized_matchings = []
    fanout = compute_fanout(matchings)
    for k, v in fanout.items():
        for onto_name, el in v.items():
            if len(el) > threshold_to_summarize:
                #print("Summarize: ")
                #for el in v:
                #    #if(el[0][2] == "qudt_units"):
                #    #    print("l")
                #    print(str(el))
                s_matching = summarize(el, handler)  # [sch - class] -> returns only 1 !
                #print("Into: ")
                #print(str(s_matching))
                #for el in set(s_matchings):
                for m in s_matching:
                    summarized_matchings.append(m)
            else:  # just propagate matchings
                for matching in el:
                    summarized_matchings.append(matching)
    return summarized_matchings


def combine_matchings(all_matchings):

    def process_attr_matching(building_matching_objects, m, matching_type):
        sch, krn = m
        db_name, source_name, field_name = sch
        kr_name, class_name = krn
        mobj = building_matching_objects.get((db_name, source_name), None)
        if mobj is None:
            mobj = Matching(db_name, source_name)
        mobj.add_attribute_correspondence(field_name, kr_name, class_name, matching_type)
        building_matching_objects[(db_name, source_name)] = mobj

    def process_relation_matching(building_matching_objects, m, matching_type):
        sch, krn = m
        db_name, source_name, field_name = sch
        kr_name, class_name = krn
        mobj = building_matching_objects.get((db_name, source_name), None)
        if mobj is None:
            mobj = Matching(db_name, source_name)
        mobj.add_relation_correspondence(kr_name, class_name, matching_type)
        building_matching_objects[(db_name, source_name)] = mobj

    l1_matchings = all_matchings[MatchingType.L1_CLASSNAME_ATTRVALUE]
    l2_matchings = all_matchings[MatchingType.L2_CLASSVALUE_ATTRVALUE]
    l4_matchings = all_matchings[MatchingType.L4_CLASSNAME_RELATIONNAME_SYN]
    l42_matchings = all_matchings[MatchingType.L42_CLASSNAME_RELATIONNAME_SEM]
    l5_matchings = all_matchings[MatchingType.L5_CLASSNAME_ATTRNAME_SYN]
    l52_matchings = all_matchings[MatchingType.L52_CLASSNAME_ATTRNAME_SEM]
    l6_matchings = all_matchings[MatchingType.L6_CLASSNAME_RELATION_SEMSIG]
    l7_matchings = all_matchings[MatchingType.L7_CLASSNAME_ATTRNAME_FUZZY]

    building_matching_objects = defaultdict(None)  # (db_name, source_name) -> stuff

    for m in l1_matchings:
        process_attr_matching(building_matching_objects, m, MatchingType.L1_CLASSNAME_ATTRVALUE)

    for m in l2_matchings:
        process_attr_matching(building_matching_objects, m, MatchingType.L2_CLASSVALUE_ATTRVALUE)

    for m in l4_matchings:
        process_relation_matching(building_matching_objects, m, MatchingType.L4_CLASSNAME_RELATIONNAME_SYN)

    for m in l42_matchings:
        process_relation_matching(building_matching_objects, m, MatchingType.L42_CLASSNAME_RELATIONNAME_SEM)

    for m in l5_matchings:
        process_attr_matching(building_matching_objects, m, MatchingType.L5_CLASSNAME_ATTRNAME_SYN)

    for m in l52_matchings:
        process_attr_matching(building_matching_objects, m, MatchingType.L52_CLASSNAME_ATTRNAME_SEM)

    for m in l6_matchings:
        process_relation_matching(building_matching_objects, m, MatchingType.L6_CLASSNAME_RELATION_SEMSIG)

    for m in l7_matchings:
        process_attr_matching(building_matching_objects, m, MatchingType.L7_CLASSNAME_ATTRNAME_FUZZY)

    return building_matching_objects


def combine_matchings2(all_matchings):
    # TODO: divide running score, based on whether content was available or not (is it really necessary?)

    # L1 creates its own matchings
    l1_matchings = all_matchings[MatchingType.L1_CLASSNAME_ATTRVALUE]

    # L2, L5, L52 and L6 create another set of matchings
    l2_matchings = all_matchings[MatchingType.L2_CLASSVALUE_ATTRVALUE]
    l5_matchings = all_matchings[MatchingType.L5_CLASSNAME_ATTRNAME_SYN]
    l52_matchings = all_matchings[MatchingType.L52_CLASSNAME_ATTRNAME_SEM]
    l6_matchings = all_matchings[MatchingType.L6_CLASSNAME_RELATION_SEMSIG]
    l7_matchings = all_matchings[MatchingType.L7_CLASSNAME_ATTRNAME_FUZZY]

    l_combined = dict()
    for schema, kr in l1_matchings:
        db_name, src_name, attr_name = schema
        kr_name, cla_name = kr
        l_combined[(db_name, src_name, attr_name, kr_name, cla_name)] = (
        (schema, kr), [MatchingType.L1_CLASSNAME_ATTRVALUE])

    for schema, kr in l7_matchings:
        db_name, src_name, attr_name = schema
        kr_name, cla_name = kr
        if (db_name, src_name, attr_name, kr_name, cla_name) in l_combined:
            l_combined[(db_name, src_name, attr_name, kr_name, cla_name)][1].append(
                MatchingType.L7_CLASSNAME_ATTRNAME_FUZZY)

    for schema, kr in l2_matchings:
        db_name, src_name, attr_name = schema
        kr_name, cla_name = kr
        if (db_name, src_name, attr_name, kr_name, cla_name) in l_combined:
            l_combined[(db_name, src_name, attr_name, kr_name, cla_name)][1].append(
                MatchingType.L2_CLASSNAME_ATTRNAME_SYN)
        else:
            l_combined[(db_name, src_name, attr_name, kr_name, cla_name)] = (
            (schema, kr), [MatchingType.L2_CLASSVALUE_ATTRVALUE])

    for schema, kr in l5_matchings:
        db_name, src_name, attr_name = schema
        kr_name, cla_name = kr
        if (db_name, src_name, attr_name, kr_name, cla_name) in l_combined:
            l_combined[(db_name, src_name, attr_name, kr_name, cla_name)][1].append(
                MatchingType.L5_CLASSNAME_ATTRNAME_SYN)
        else:
            l_combined[(db_name, src_name, attr_name, kr_name, cla_name)] = (
            (schema, kr), [MatchingType.L5_CLASSNAME_ATTRNAME_SYN])

    for schema, kr in l52_matchings:
        db_name, src_name, attr_name = schema
        kr_name, cla_name = kr
        if (db_name, src_name, attr_name, kr_name, cla_name) in l_combined:
            l_combined[(db_name, src_name, attr_name, kr_name, cla_name)][1].append(
                MatchingType.L52_CLASSNAME_ATTRNAME_SEM)
        else:
            l_combined[(db_name, src_name, attr_name, kr_name, cla_name)] = (
            (schema, kr), [MatchingType.L52_CLASSNAME_ATTRNAME_SEM])

    for schema, kr in l6_matchings:
        db_name, src_name, attr_name = schema
        kr_name, cla_name = kr
        if (db_name, src_name, attr_name, kr_name, cla_name) in l_combined:
            # TODO: only append in the matching types are something except L1?
            l_combined[(db_name, src_name, attr_name, kr_name, cla_name)][1].append(
                MatchingType.L6_CLASSNAME_RELATION_SEMSIG)

    # L4 and L42 have their own matching too
    l4_matchings = all_matchings[MatchingType.L4_CLASSNAME_RELATIONNAME_SYN]
    combined_matchings = []
    for key, values in l_combined.items():
        matching = values[0]
        matching_types = values[1]
        # for el in values:
        #    matching = el[0]
        #    matching_types = el[1]
        combined_matchings.append((matching, matching_types))

    combined_matchings = sorted(combined_matchings, key=lambda x: len(x[1]), reverse=True)

    return combined_matchings, l4_matchings


def removed_banned_vectors (ban_index, svs):
    nSVS  = []
    for iter1 in range(0, len(ban_index)):
        if ban_index[iter1] == 0: # the corresponding vector is not banned
            if iter1 < len(svs):
                nSVS.append(svs[iter1])
        iter1 += 1
    return nSVS


def get_ban_indexes(relation1, relation2):
    relation1 = nlp.camelcase_to_snakecase(relation1)
    relation1 = relation1.replace('-', ' ')
    relation1 = relation1.replace('_', ' ')
    relation1 = relation1.lower()

    relation2 = nlp.camelcase_to_snakecase(relation2)
    relation2 = relation2.replace('-', ' ')
    relation2 = relation2.replace('_', ' ')
    relation2 = relation2.lower()

    if relation1 is not None and relation2 is not None:
        ban_index1 = [0] * len(relation1.split())
        ban_index2 = [0] * len(relation2.split())
        iter1 = 0
        for token1 in relation1.split():
            iter2 = 0
            for token2 in relation2.split():
                if token1 == token2:
                    ban_index1[iter1] = 1
                    ban_index2[iter2] = 1
                iter2 += 1
            iter1 += 1
        return ban_index1, ban_index2


def find_relation_class_attr_name_sem_matchings(network, kr_handlers,
                                                semantic_sim_threshold=0.5,
                                                sensitivity_neg_signal=0.5,
                                                negative_signal_threshold=0.4,
                                                penalize_unknown_word=False,
                                                add_exact_matches=True):
    # Retrieve relation names
    st = time.time()
    names = []
    seen_fields = set()
    for (db_name, source_name, field_name, _) in network.iterate_values():
        orig_field_name = field_name
        key_seen = source_name + field_name
        if key_seen not in seen_fields:
            seen_fields.add(key_seen)  # seen already
            field_name = nlp.camelcase_to_snakecase(field_name)
            field_name = field_name.replace('-', ' ')
            field_name = field_name.replace('_', ' ')
            field_name = field_name.lower()
            svs = []
            for token in field_name.split():
                if token not in stopwords.words('english'):
                    sv = glove_api.get_embedding_for_word(token)
                    if sv is not None:
                        svs.append(sv)
            names.append(('attribute', (db_name, source_name, orig_field_name), svs))

    num_attributes_inserted = len(names)

    # Retrieve class names
    for kr_name, kr_handler in kr_handlers.items():
        all_classes = kr_handler.classes()
        for cl in all_classes:
            original_cl_name = cl
            #cl = nlp.camelcase_to_snakecase(cl)
            cl = cl.replace('-', ' ')
            cl = cl.replace('_', ' ')
            cl = cl.lower()
            svs = []
            for token in cl.split():
                if token not in stopwords.words('english'):
                    sv = glove_api.get_embedding_for_word(token)
                    if sv is not None:
                        svs.append(sv)
            names.append(('class', (kr_name, original_cl_name), svs))

    pos_matchings = []
    neg_matchings = []
    for idx_rel in range(0, num_attributes_inserted):  # Compare only with classes
        for idx_class in range(num_attributes_inserted, len(names)):
            ban_index1, ban_index2 = get_ban_indexes(names[idx_rel][1][2], names[idx_class][1][1])
            svs_rel = removed_banned_vectors(ban_index1, names[idx_rel][2])
            svs_cla = removed_banned_vectors(ban_index2, names[idx_class][2])
            semantic_sim, strong_signal = SS.compute_semantic_similarity(svs_rel, svs_cla,
                                    penalize_unknown_word=penalize_unknown_word,
                                    add_exact_matches=add_exact_matches,
                                    signal_strength_threshold=sensitivity_neg_signal)
            if strong_signal and semantic_sim > semantic_sim_threshold:
                # match.format db_name, source_name, field_name -> class_name
                match = ((names[idx_rel][1][0], names[idx_rel][1][1], names[idx_rel][1][2]), names[idx_class][1])
                pos_matchings.append(match)
            elif strong_signal and semantic_sim < negative_signal_threshold:
                match = ((names[idx_rel][1][0], names[idx_rel][1][1], names[idx_rel][1][2]), names[idx_class][1])
                neg_matchings.append(match)
    et = time.time()
    print("l52: " + str(et - st))
    return pos_matchings, neg_matchings


def find_relation_class_attr_name_matching(network, kr_handlers, minhash_sim_threshold=0.5):
    # Retrieve relation names
    st = time.time()
    names = []
    seen_fields = set()
    for (db_name, source_name, field_name, _) in network.iterate_values():
        orig_field_name = field_name
        key_seen = source_name + field_name
        if key_seen not in seen_fields:
            seen_fields.add(key_seen)  # seen already
            field_name = nlp.camelcase_to_snakecase(field_name)
            field_name = field_name.replace('-', ' ')
            field_name = field_name.replace('_', ' ')
            field_name = field_name.lower()
            m = MinHash(num_perm=64)
            for token in field_name.split():
                if token not in stopwords.words('english'):
                    m.update(token.encode('utf8'))
            names.append(('attribute', (db_name, source_name, orig_field_name), m))

    num_attributes_inserted = len(names)

    # Retrieve class names
    for kr_name, kr_handler in kr_handlers.items():
        all_classes = kr_handler.classes()
        for cl in all_classes:
            if cl == "SMILES" or cl == "InChI":
                print("a")
            original_cl_name = cl
            #cl = nlp.camelcase_to_snakecase(cl)
            cl = cl.replace('-', ' ')
            cl = cl.replace('_', ' ')
            cl = cl.lower()
            m = MinHash(num_perm=64)
            for token in cl.split():
                if token not in stopwords.words('english'):
                    m.update(token.encode('utf8'))
            names.append(('class', (kr_name, original_cl_name), m))

    # Index all the minhashes
    lsh_index = MinHashLSH(threshold=minhash_sim_threshold, num_perm=64)

    for idx in range(len(names)):
        lsh_index.insert(idx, names[idx][2])

    matchings = []
    for idx in range(0, num_attributes_inserted):  # Compare only with classes
        if names[idx][1][2] == "standard_inchi" or names[idx][1][2] == "standard_inchi_key":
            print("B")
        N = lsh_index.query(names[idx][2])
        for n in N:
            kind_q = names[idx][0]
            kind_n = names[n][0]
            if kind_n != kind_q:
                # match.format db_name, source_name, field_name -> class_name
                match = ((names[idx][1][0], names[idx][1][1], names[idx][1][2]), names[n][1])
                matchings.append(match)
    et = time.time()
    print("l5: " + str((et-st)))
    return matchings


def find_relation_class_name_sem_matchings(network, kr_handlers,
                                           sem_sim_threshold=0.5,
                                           negative_signal_threshold=0.4,
                                           sensitivity_neg_signal=0.5,
                                           penalize_unknown_word=False,
                                           add_exact_matches=True):
    # Retrieve relation names
    st = time.time()
    names = []
    seen_sources = set()
    for (db_name, source_name, _, _) in network.iterate_values():
        original_source_name = source_name
        if source_name not in seen_sources:
            seen_sources.add(source_name)  # seen already
            source_name = source_name.replace('-', ' ')
            source_name = source_name.replace('_', ' ')
            source_name = source_name.lower()
            svs = []
            for token in source_name.split():
                if token not in stopwords.words('english'):
                    sv = glove_api.get_embedding_for_word(token)
                    #if sv is not None:
                    svs.append(sv)  # append even None, to apply penalization later
            names.append(('relation', (db_name, original_source_name), svs))

    num_relations_inserted = len(names)

    # Retrieve class names
    for kr_name, kr_handler in kr_handlers.items():
        all_classes = kr_handler.classes()
        for cl in all_classes:
            original_cl_name = cl
            cl = nlp.camelcase_to_snakecase(cl)
            cl = cl.replace('-', ' ')
            cl = cl.replace('_', ' ')
            cl = cl.lower()
            svs = []
            for token in cl.split():
                if token not in stopwords.words('english'):
                    sv = glove_api.get_embedding_for_word(token)
                    #if sv is not None:
                    svs.append(sv)  # append even None, to apply penalization later
            names.append(('class', (kr_name, original_cl_name), svs))
    pos_matchings = []  # evidence for a real matching
    neg_matchings = []  # evidence that this matching probably is wrong
    for idx_rel in range(0, num_relations_inserted):  # Compare only with classes
        for idx_class in range(num_relations_inserted, len(names)):
            ban_index1, ban_index2 = get_ban_indexes(names[idx_rel][1][1], names[idx_class][1][1])
            svs_rel = removed_banned_vectors(ban_index1, names[idx_rel][2])
            svs_cla = removed_banned_vectors(ban_index2, names[idx_class][2])
            semantic_sim, strong_signal = SS.compute_semantic_similarity(svs_rel, svs_cla,
                                            penalize_unknown_word=penalize_unknown_word,
                                            add_exact_matches=add_exact_matches,
                                            signal_strength_threshold=sensitivity_neg_signal)
            if strong_signal and semantic_sim > sem_sim_threshold:
                # match.format is db_name, source_name, field_name -> class_name
                match = ((names[idx_rel][1][0], names[idx_rel][1][1], "_"), names[idx_class][1])
                pos_matchings.append(match)
            elif strong_signal and semantic_sim < negative_signal_threshold:
                match = ((names[idx_rel][1][0], names[idx_rel][1][1], "_"), names[idx_class][1])
                neg_matchings.append(match)
    et = time.time()
    print("l42: " + str(et - st))
    return pos_matchings, neg_matchings


def find_relation_class_name_matchings(network, kr_handlers, minhash_sim_threshold=0.5):
    # Retrieve relation names
    st = time.time()
    names = []
    seen_sources = set()
    for (db_name, source_name, _, _) in network.iterate_values():
        original_source_name = source_name
        if source_name not in seen_sources:
            seen_sources.add(source_name)  # seen already
            source_name = nlp.camelcase_to_snakecase(source_name)
            source_name = source_name.replace('-', ' ')
            source_name = source_name.replace('_', ' ')
            source_name = source_name.lower()
            m = MinHash(num_perm=32)
            for token in source_name.split():
                if token not in stopwords.words('english'):
                    m.update(token.encode('utf8'))
            names.append(('relation', (db_name, original_source_name), m))

    num_relations_inserted = len(names)

    # Retrieve class names
    for kr_name, kr_handler in kr_handlers.items():
        all_classes = kr_handler.classes()
        for cl in all_classes:
            original_cl_name = cl
            cl = nlp.camelcase_to_snakecase(cl)
            cl = cl.replace('-', ' ')
            cl = cl.replace('_', ' ')
            cl = cl.lower()
            m = MinHash(num_perm=32)
            for token in cl.split():
                if token not in stopwords.words('english'):
                    m.update(token.encode('utf8'))
            names.append(('class', (kr_name, original_cl_name), m))

    # Index all the minhashes
    lsh_index = MinHashLSH(threshold=minhash_sim_threshold, num_perm=32)

    for idx in range(len(names)):
        lsh_index.insert(idx, names[idx][2])

    matchings = []
    for idx in range(0, num_relations_inserted):  # Compare only with classes
        N = lsh_index.query(names[idx][2])
        for n in N:
            kind_q = names[idx][0]
            kind_n = names[n][0]
            if kind_n != kind_q:
                # match.format is db_name, source_name, field_name -> class_name
                match = ((names[idx][1][0], names[idx][1][1], "_"), names[n][1])
                matchings.append(match)
    et = time.time()
    print("l4: " + str(et - st))
    return matchings


def __find_relation_class_matchings(self):
    # Retrieve relation names
    st = time.time()
    docs = []
    names = []
    seen_sources = []
    for (_, source_name, _, _) in self.network.iterate_values():
        if source_name not in seen_sources:
            seen_sources.append(source_name)  # seen already
            source_name = source_name.replace('-', ' ')
            source_name = source_name.replace('_', ' ')
            source_name = source_name.lower()
            docs.append(source_name)
            names.append(('relation', source_name))

    # Retrieve class names
    for kr_item, kr_handler in self.kr_handlers.items():
        all_classes = kr_handler.classes()
        for cl in all_classes:
            cl = cl.replace('-', ' ')
            cl = cl.replace('_', ' ')
            cl = cl.lower()
            docs.append(cl)
            names.append(('class', cl))

    tfidf = da.get_tfidf_docs(docs)
    et = time.time()
    print("Time to create docs and TF-IDF: ")
    print("Create docs and TF-IDF: {0}".format(str(et - st)))

    num_features = tfidf.shape[1]
    new_index_engine = LSHRandomProjectionsIndex(num_features, projection_count=7)

    # N2 method
    """
    clean_matchings = []
    for i in range(len(docs)):
        for j in range(len(docs)):
            sparse_row = tfidf.getrow(i)
            dense_row = sparse_row.todense()
            array_i = dense_row.A[0]

            sparse_row = tfidf.getrow(j)
            dense_row = sparse_row.todense()
            array_j = dense_row.A[0]

            sim = np.dot(array_i, array_j.T)
            if sim > 0.5:
                if names[i][0] != names[j][0]:
                    match = names[i][1], names[j][1]
                    clean_matchings.append(match)
    return clean_matchings
    """

    # Index vectors in engine
    st = time.time()

    for idx in range(len(docs)):
        sparse_row = tfidf.getrow(idx)
        dense_row = sparse_row.todense()
        array = dense_row.A[0]
        new_index_engine.index(array, idx)
    et = time.time()
    print("Total index text: " + str((et - st)))

    # Now query for similar ones:
    raw_matchings = defaultdict(list)
    for idx in range(len(docs)):
        sparse_row = tfidf.getrow(idx)
        dense_row = sparse_row.todense()
        array = dense_row.A[0]
        N = new_index_engine.query(array)
        if len(N) > 1:
            for n in N:
                (data, key, value) = n
                raw_matchings[idx].append(key)
    et = time.time()
    print("Find raw matches: {0}".format(str(et - st)))

    # Filter matches so that only relation-class ones appear
    clean_matchings = []
    for key, values in raw_matchings.items():
        key_kind = names[key][0]
        for v in values:
            v_kind = names[v][0]
            if v_kind != key_kind:
                match = (names[key][1], names[v][1])
                clean_matchings.append(match)
    return clean_matchings


def find_sem_coh_matchings(network, kr_handlers, sem_sim_threshold=0.7, group_size_cutoff=2):
    st = time.time()
    matchings = []
    matchings_special = []
    # Get all relations with groups
    table_groups = dict()
    for db, t, attrs in SS.read_table_columns(None, network=network):
        groups = SS.extract_cohesive_groups(t, attrs, sem_sim_threshold=sem_sim_threshold, group_size_cutoff=group_size_cutoff)
        table_groups[(db, t)] = groups  # (score, [set()])

    names = []
    # Retrieve class names
    for kr_name, kr_handler in kr_handlers.items():
        all_classes = kr_handler.classes()
        for cl in all_classes:
            original_cl_name = cl
            cl = nlp.camelcase_to_snakecase(cl)
            cl = cl.replace('-', ' ')
            cl = cl.replace('_', ' ')
            cl = cl.lower()
            svs = []
            for token in cl.split():
                if token not in stopwords.words('english'):
                    sv = glove_api.get_embedding_for_word(token)
                    if sv is not None:
                        svs.append(sv)
            names.append(('class', (kr_name, original_cl_name), svs))

    for db_table_info, groups in table_groups.items():
        db_name, table_name = db_table_info
        class_seen = []  # to filter out already seen classes
        for g_score, g_tokens in groups:
            if len(g_tokens) == 0:
                continue
            g_svs = []
            for t in g_tokens:
                sv = glove_api.get_embedding_for_word(t)
                if sv is not None:
                    g_svs.append(sv)
            for _, class_info, class_svs in names:
                kr_name, class_name = class_info
                sim, strong_signal = SS.compute_semantic_similarity(class_svs, g_svs,
                                                                    penalize_unknown_word=True,
                                                                    add_exact_matches=False,
                                                                    signal_strength_threshold=0.5)
                if sim > g_score and class_name not in class_seen:
                    class_seen.append(class_name)
                    match = ((db_name, table_name, "_"), (kr_name, class_name))
                    matchings.append(match)
                """
                similar = SS.groupwise_semantic_sim(class_svs, g_svs, 0.7)
                if similar:
                    class_seen.append(class_name)
                    match = ((db_name, table_name, "_"), (kr_name, class_name))
                    matchings_special.append(match)
                continue
                """
    et = time.time()
    print("l6: " + str((et-st)))
    return matchings, table_groups  #, matchings_special

cutoff_likely_match_threshold = 0.4
min_relevance_score = 0.2
scoring_threshold = 0.4
min_classes = 50


def find_hierarchy_content_fuzzy(kr_handlers, store):
    matchings = []
    # access class names, per hierarchical level (this is one assumption that makes sense)
    for kr_name, kr in kr_handlers.items():
        ch = kr.class_hierarchy
        for ch_name, ch_classes in ch:
            if len(ch_classes) < min_classes:  # do this only for longer hierarchies
                continue
            # query elastic for fuzzy matches
            matching_evidence = defaultdict(int)
            for class_id, class_name in ch_classes:
                matches = store.fuzzy_keyword_match(class_name)
                keys_in_matches = set()
                for m in matches:
                    # record
                    if m.score > min_relevance_score:
                        key = (m.db_name, m.source_name, m.field_name)
                        keys_in_matches.add(key)
                for k in keys_in_matches:
                    matching_evidence[k] += 1
            num_classes = len(ch_classes)
            num_potential_matches = len(matching_evidence.items())
            cutoff_likely_match = float(num_potential_matches/num_classes)
            if cutoff_likely_match > cutoff_likely_match_threshold:  # if passes cutoff threshold then
                continue
            sorted_matching_evidence = sorted(matching_evidence.items(), key=operator.itemgetter(1), reverse=True)
            # a perfect match would score 1
            for key, value in sorted_matching_evidence:
                score = float(value/num_classes)
                if score > scoring_threshold:
                    match = (key, (kr_name, ch_name))
                    matchings.append(match)
                else:
                    break  # orderd, so once one does not comply, no one else does...
    return matchings


def find_relation_class_sem_coh_clss_context(network, kr_handlers):

    matchings = []

    # consider reusing this from previous execution...
    table_groups = dict()
    for db, t, attrs in SS.read_table_columns(None, network=network):
        groups = SS.extract_cohesive_groups(t, attrs)
        table_groups[(db, t)] = groups  # (score, [set()])

    # get vectors for onto classes
    class_ss = _get_kr_classes_vectors(kr_handlers)

    for db_table_info, groups in table_groups.items():
        db_name, table_name = db_table_info
        class_seen = []  # to filter out already seen classes
        for g_score, g_tokens in groups:
            g_svs = []
            for t in g_tokens:
                sv = glove_api.get_embedding_for_word(t)
                if sv is not None:
                    g_svs.append(sv)

            for class_info, class_svs in class_ss.items():
                kr_name, class_name = class_info
                sim, strong_signal = SS.compute_semantic_similarity(class_svs, g_svs)
                if sim > g_score and class_name not in class_seen:
                    class_seen.append(class_name)
                    match = ((db_name, table_name, "_"), (kr_name, class_name))
                    matchings.append(match)
    return matchings


def _get_kr_classes_vectors(kr_handlers):
    class_vectors = dict()
    for kr_name, kr in kr_handlers.items():
        for class_name in kr.classes_id():
            success, ret = kr.bow_repr_of(class_name, class_id=True)  # Get bag of words representation
            if success:
                label, bow = ret
                seen_tokens = []  # filtering out already seen tokens
                sem_vectors = []
                for el in bow:
                    el = el.replace('_', ' ')
                    tokens = el.split(' ')
                    for token in tokens:
                        token = token.lower()
                        if token not in stopwords.words('english'):
                            seen_tokens.append(token)
                            sem_vector = glove_api.get_embedding_for_word(token)
                            if sem_vector is not None:
                                sem_vectors.append(sem_vector)
                if len(sem_vectors) > 0:  # otherwise just no context generated for this class
                    class_vectors[(kr_name, kr.name_of_class(class_name))] = sem_vectors
    return class_vectors


def adhoc_test():
    from inputoutput import inputoutput as io
    from knowledgerepr import fieldnetwork
    from modelstore.elasticstore import StoreHandler
    from ontomatch.ss_api import SSAPI

    s1 = "chembl_22 %%% assay_type %%% assay_desc ==>> efo %%% Motility Assay %%% [ < MatchingType.L5_CLASSNAME_ATTRNAME_SYN: 5 >]"
    s2 = "chembl_22 %%% assay_type %%% assay_desc ==>> efo %%% Assay By Sequencer %%% [ < MatchingType.L5_CLASSNAME_ATTRNAME_SYN: 5 >]"
    s3 = "chembl_22 %%% assay_type %%% assay_desc ==>> efo %%% Microscopy Assay %%% [ < MatchingType.L5_CLASSNAME_ATTRNAME_SYN: 5 >]"
    s4 = "chembl_22 %%% assay_type %%% assay_desc ==>> efo %%% Chromosome Conformation Capture Assay %%% [ < MatchingType.L5_CLASSNAME_ATTRNAME_SYN: 5 >]"
    s5 = "chembl_22 %%% assay_type %%% assay_desc ==>> efo %%% Assay By Molecule %%% [ < MatchingType.L5_CLASSNAME_ATTRNAME_SYN: 5 >]"
    s6 = "chembl_22 %%% assay_type %%% assay_desc ==>> efo %%% Assay By Array %%% [ < MatchingType.L5_CLASSNAME_ATTRNAME_SYN: 5 >]"
    s7 = "chembl_22 %%% assay_type %%% assay_desc ==>> efo %%% Assay By High Throughput Sequencer %%% [ < MatchingType.L5_CLASSNAME_ATTRNAME_SYN: 5 >]"
    s8 = "chembl_22 %%% assay_type %%% assay_desc ==>> efo %%% Protein Assay %%% [ < MatchingType.L5_CLASSNAME_ATTRNAME_SYN: 5 >]"
    s9 = "chembl_22 %%% assay_type %%% assay_desc ==>> efo %%% Dna Assay %%% [ < MatchingType.L5_CLASSNAME_ATTRNAME_SYN: 5 >]"
    s10 = "chembl_22 %%% assay_type %%% assay_desc ==>> efo %%% Population Growth Assay %%% [ < MatchingType.L5_CLASSNAME_ATTRNAME_SYN: 5 >]"
    s11 = "chembl_22 %%% assay_type %%% assay_desc ==>> efo %%% Assay By Instrument %%% [ < MatchingType.L5_CLASSNAME_ATTRNAME_SYN: 5 >]"
    s12 = "chembl_22 %%% assay_type %%% assay_desc ==>> efo %%% Assay By Mass Spectrometry %%% [ < MatchingType.L5_CLASSNAME_ATTRNAME_SYN: 5 >]"
    s13 = "chembl_22 %%% assay_type %%% assay_desc ==>> efo %%% Finishing Sequencing Assay %%% [ < MatchingType.L5_CLASSNAME_ATTRNAME_SYN: 5 >]"
    s14 = "chembl_22 %%% assay_type %%% assay_desc ==>> efo %%% Rna Assay %%% [ < MatchingType.L5_CLASSNAME_ATTRNAME_SYN: 5 >]"
    s15 = "chembl_22 %%% assay_type %%% assay_desc ==>> efo %%% Assay %%% [ < MatchingType.L5_CLASSNAME_ATTRNAME_SYN: 5 >]"
    matchings_str = [s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15]

    matchings = []
    for m in matchings_str:
        tokens = m.split("==>>")
        sch = tokens[0]
        cla = tokens[1]
        sch_tokens = sch.split("%%%")
        db, r, attr = sch_tokens[0].strip(), sch_tokens[1].strip(), sch_tokens[2].strip()
        cla_tokens = cla.split("%%%")
        o_name, cla_name = cla_tokens[0].strip(), cla_tokens[1].strip()
        match = ((db, r, attr), (o_name, cla_name))
        matchings.append(match)

    # Deserialize model
    network = fieldnetwork.deserialize_network("../models/chembl22/")
    # Create client
    store_client = StoreHandler()

    # Retrieve indexes
    schema_sim_index = io.deserialize_object("../models/chembl22/" + 'schema_sim_index.pkl')
    content_sim_index = io.deserialize_object("../models/chembl22/" + 'content_sim_index.pkl')

    # Create ontomatch api
    om = SSAPI(network, store_client, schema_sim_index, content_sim_index)
    om.add_krs([("efo", "cache_onto/efo.pkl")], parsed=True)

    # class_name = "Motility Assay"
    # root_to_class_name = om.kr_handlers["efo"].ancestors_of_class(class_name)
    # root_to_class_name = om.kr_handlers["efo"].name_of_sequence(root_to_class_name)

    sum_matchings = summarize_matchings_to_ancestor(om.kr_handlers["efo"], matchings)

    print("Original: " + str(len(matchings)))
    print("Summary: " + str(len(sum_matchings)))

    for m in sum_matchings:
        print(m)

if __name__ == "__main__":
    print("Matcher lib")

    #adhoc_test()

    from inputoutput import inputoutput as io
    from knowledgerepr import fieldnetwork
    from modelstore.elasticstore import StoreHandler
    from ontomatch.ss_api import SSAPI

    matchings = []
    with open("results/basic_05", 'r') as f:
        lines = f.readlines()
        for l in lines:
            tokens = l.split("==>>")
            sch = tokens[0]
            cla = tokens[1]
            sch_tokens = sch.split("%%%")
            sch_tokens = [t.strip() for t in sch_tokens]
            cla_tokens = cla.split("%%%")
            cla_tokens = [t.strip() for t in cla_tokens]
            matching_format = ((sch_tokens[0], sch_tokens[1], sch_tokens[2]), (cla_tokens[0], cla_tokens[1]))
            matchings.append(matching_format)

    # Deserialize model
    network = fieldnetwork.deserialize_network("../models/chembl22/")
    # Create client
    store_client = StoreHandler()

    # Retrieve indexes
    schema_sim_index = io.deserialize_object("../models/chembl22/" + 'schema_sim_index.pkl')
    content_sim_index = io.deserialize_object("../models/chembl22/" + 'content_sim_index.pkl')

    # Create ontomatch api
    om = SSAPI(network, store_client, schema_sim_index, content_sim_index)
    om.add_krs([("efo", "cache_onto/efo.pkl")], parsed=True)

    sum_matchings = summarize_matchings_to_ancestor(om.kr_handlers["efo"], matchings)

    print("Original: " + str(len(matchings)))
    print("Summary: " + str(len(sum_matchings)))

    #sum_matchings2 = summarize_matchings_to_ancestor(om.kr_handlers["efo"], sum_matchings)

    #print("Summary2: " + str(len(sum_matchings2)))

    #for s in sum_matchings:
    #    print(s)
