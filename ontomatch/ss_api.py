import sys
from algebra import API
from knowledgerepr import fieldnetwork
from modelstore.elasticstore import StoreHandler
from ontomatch import ss_utils as SS
from knowledgerepr.networkbuilder import LSHRandomProjectionsIndex
from ontomatch import glove_api
from collections import defaultdict
from ontomatch.onto_parser import OntoHandler
from inputoutput import inputoutput as io
from datasketch import MinHash, MinHashLSH
import numpy as np
from nltk.corpus import stopwords
from dataanalysis import nlp_utils as nlp
import time
from dataanalysis import dataanalysis as da
from enum import Enum
from modelstore import elasticstore as store

# Have a list of accepted formats in the ontology parser


class MatchingType(Enum):
    L1_CLASSNAME_ATTRVALUE = 0
    L2_CLASSVALUE_ATTRVALUE = 1
    L3_CLASSCTX_RELATIONCTX = 2
    L4_CLASSNAME_RELATIONNAME_SYN = 3
    L42_CLASSNAME_RELATIONNAME_SEM = 4
    L5_CLASSNAME_ATTRNAME_SYN = 5
    L52_CLASSNAME_ATTRNAME_SEM = 6
    L6_CLASSNAME_RELATION_SEMSIG = 7


class SSAPI:

    def __init__(self, network, store_client, schema_sim_index, content_sim_index):
        self.network = network
        self.store_client = store_client
        self.schema_sim_index = schema_sim_index
        self.content_sim_index = content_sim_index
        self.srql = API(self.network, self.store_client)
        self.krs = []
        self.kr_handlers = dict()

        # SS indexes
        self.num_vectors_tables = 0  # Total number of semantic vectors for tables
        self.sskey_to_ss = dict()  # ss_key to (table_name, semantic_vectors)
        self.sskey_to_vkeys = dict()  # ss_key to [vector_keys]
        self.vkey_to_sskey = dict()  # vector_key to ss_key (the ss_key that contains vector_key)
        vector_feature_dim = glove_api.get_lang_model_feature_size()
        self.ss_lsh_idx = LSHRandomProjectionsIndex(vector_feature_dim)

    def add_krs(self, kr_name_paths, parsed=True):
        """
        Register the given KR for processing. Validate accepted format, etc
        # TODO: make this more usable
        :param krs: the list of (kr_name, kr_path)
        :return:
        """
        for kr_name, kr_path in kr_name_paths:
            self.krs.append((kr_name, kr_path))
            o = OntoHandler()
            if parsed:  # the ontology was preprocessed
                o.load_ontology("cache_onto/" + kr_name + ".pkl")
            else:
                o.parse_ontology(kr_path)
                o.store_ontology("cache_onto/" + kr_name + ".pkl")
            self.kr_handlers[kr_name] = o

    def __compare_content_signatures(self, kr_name, signatures):
        positive_matches = []
        for class_name, mh_sig in signatures:
            mh_obj = MinHash(num_perm=512)
            mh_array = np.asarray(mh_sig, dtype=int)
            mh_obj.hashvalues = mh_array
            res = self.content_sim_index.query(mh_obj)
            for r_nid in res:
                (nid, db_name, source_name, field_name) = self.network.get_info_for([r_nid])[0]
                # matching from db attr to name
                matching = ((db_name, source_name, field_name), (kr_name, class_name))
                positive_matches.append(matching)
        return positive_matches

    def __build_content_sim(self, threshold):
        # Build a content similarity index
        # Content_sim text relation (minhash-based)
        start_text_sig_sim = time.time()
        st = time.time()
        mh_signatures = self.store_client.get_all_mh_text_signatures()
        et = time.time()
        print("Time to extract minhash signatures from store: {0}".format(str(et - st)))
        print("!!3 " + str(et - st))

        content_index = MinHashLSH(threshold=threshold, num_perm=512)
        mh_sig_obj = []
        # Create minhash objects and index
        for nid, mh_sig in mh_signatures:
            mh_obj = MinHash(num_perm=512)
            mh_array = np.asarray(mh_sig, dtype=int)
            mh_obj.hashvalues = mh_array
            content_index.insert(nid, mh_obj)
            mh_sig_obj.append((nid, mh_obj))
        end_text_sig_sim = time.time()
        print("Total text-sig-sim (minhash): {0}".format(str(end_text_sig_sim - start_text_sig_sim)))
        print("!!4 " + str(end_text_sig_sim - start_text_sig_sim))

        self.content_sim_index = content_index

    def find_matchings(self):
        """
        Find matching for each of the different possible categories
        :return: list of matchings
        """
        all_matchings = dict()

        # Build content sim
        self.__build_content_sim(0.6)

        # L1: [class] -> attr.content
        st = time.time()
        print("Finding L1 matchings...")
        kr_class_signatures = []
        l1_matchings = []
        for kr_name, kr_handler in self.kr_handlers.items():
            kr_class_signatures += kr_handler.get_clhashasses_signatures(filter=5)
            l1_matchings += self.__compare_content_signatures(kr_name, kr_class_signatures)

        print("Finding L1 matchings...OK, "+str(len(l1_matchings))+" found")
        et = time.time()
        print("Took: " + str(et-st))
        all_matchings[MatchingType.L1_CLASSNAME_ATTRVALUE] = l1_matchings

        for match in l1_matchings:
            print(match)

        exit()

        # L2: [class.data] -> attr.content
        print("Finding L2 matchings...")
        st = time.time()
        kr_classdata_signatures = []
        l2_matchings = []
        #for kr_name, kr_handler in self.kr_handlers.items():
        #    kr_classdata_signatures += kr_handler.get_class_data_signatures()
        #    l2_matchings = self.__compare_content_signatures(kr_name, kr_classdata_signatures)
        print("Finding L2 matchings...OK, " + str(len(l2_matchings)) + " found")
        et = time.time()
        print("Took: " + str(et - st))
        all_matchings[MatchingType.L2_CLASSVALUE_ATTRVALUE] = l2_matchings

        #for match in l2_matchings:
        #    print(match)

        # L3: [class.context] -> relation
        print("Finding L3 matchings...")
        st = time.time()
        #l3_matchings = self.find_coarse_grain_hooks_n2()
        l3_matchings = []
        print("Finding L3 matchings...OK, " + str(len(l3_matchings)) + " found")
        et = time.time()
        print("Took: " + str(et - st))
        all_matchings[MatchingType.L3_CLASSCTX_RELATIONCTX] = l3_matchings

        #for match in l3_matchings:
        #    print(match)

        # L4: [Relation names] -> [Class names] (syntax)
        print("Finding L4 matchings...")
        st = time.time()
        l4_matchings = self.find_relation_class_name_matchings()
        print("Finding L4 matchings...OK, " + str(len(l4_matchings)) + " found")
        et = time.time()
        print("Took: " + str(et - st))
        all_matchings[MatchingType.L4_CLASSNAME_RELATIONNAME_SYN] = l4_matchings

        #for match in l4_matchings:
        #    print(match)

        # L4.2: [Relation names] -> [Class names] (semantic)
        print("Finding L42 matchings...")
        st = time.time()
        l42_matchings = self.find_relation_class_name_sem_matchings()
        print("Finding L42 matchings...OK, " + str(len(l42_matchings)) + " found")
        et = time.time()
        print("Took: " + str(et - st))
        all_matchings[MatchingType.L42_CLASSNAME_RELATIONNAME_SEM] = l42_matchings

        #for match in l42_matchings:
        #    print(match)

        # L5: [Attribute names] -> [Class names] (syntax)
        print("Finding L5 matchings...")
        st = time.time()
        l5_matchings = self.find_relation_class_attr_name_matching()
        print("Finding L5 matchings...OK, " + str(len(l5_matchings)) + " found")
        et = time.time()
        print("Took: " + str(et - st))
        all_matchings[MatchingType.L5_CLASSNAME_ATTRNAME_SYN] = l5_matchings

        #for match in l5_matchings:
        #    print(match)

        # L52: [Attribute names] -> [Class names] (semantic)
        print("Finding L52 matchings...")
        st = time.time()
        l52_matchings = self.find_relation_class_attr_name_sem_matchings()
        print("Finding L52 matchings...OK, " + str(len(l52_matchings)) + " found")
        et = time.time()
        print("Took: " + str(et - st))
        all_matchings[MatchingType.L52_CLASSNAME_ATTRNAME_SEM] = l52_matchings

        #for match in l52_matchings:
        #    print(match)

        # L6: [Relations] -> [Class names] (semantic groups)
        print("Finding L6 matchings...")
        st = time.time()
        l6_matchings = self.find_sem_coh_matchings()
        print("Finding L6 matchings...OK, " + str(len(l6_matchings)) + " found")
        et = time.time()
        print("Took: " + str(et - st))
        all_matchings[MatchingType.L6_CLASSNAME_RELATION_SEMSIG] = l6_matchings

        # for match in l52_matchings:
        #    print(match)

        total_matchings_pre_combined = 0
        for values in all_matchings.values():
            total_matchings_pre_combined += len(values)
        print("ALL: " + str(total_matchings_pre_combined))
        combined_matchings, l4_matchings = self._combine_matchings(all_matchings)
        print("COM: " + str(len(combined_matchings)))

        print("l6 matchings")
        for m in l6_matchings:
            print(m)

        return combined_matchings

    def _combine_matchings(self, all_matchings):
        # TODO: divide running score, based on whether content was available or not (is it really necessary?)

        # L1 creates its own matchings
        l1_matchings = all_matchings[MatchingType.L1_CLASSNAME_ATTRVALUE]

        # L2, L5, L52 and L6 create another set of matchings
        l2_matchings = all_matchings[MatchingType.L2_CLASSVALUE_ATTRVALUE]
        l5_matchings = all_matchings[MatchingType.L5_CLASSNAME_ATTRNAME_SYN]
        l52_matchings = all_matchings[MatchingType.L52_CLASSNAME_ATTRNAME_SEM]
        l6_matchings = all_matchings[MatchingType.L6_CLASSNAME_RELATION_SEMSIG]

        l_combined = dict()
        for schema, kr in l1_matchings:
            db_name, src_name, attr_name = schema
            kr_name, cla_name = kr
            l_combined[(db_name, src_name, attr_name, kr_name, cla_name)] = ((schema, kr), [MatchingType.L1_CLASSVALUE_ATTRVALUE])

        for schema, kr in l2_matchings:
            db_name, src_name, attr_name = schema
            kr_name, cla_name = kr
            if (db_name, src_name, attr_name, kr_name, cla_name) in l_combined:
                l_combined[(db_name, src_name, attr_name, kr_name, cla_name)][1].append(MatchingType.L2_CLASSNAME_ATTRNAME_SYN)
            else:
                l_combined[(db_name, src_name, attr_name, kr_name, cla_name)] = ((schema, kr), [MatchingType.L2_CLASSVALUE_ATTRVALUE])

        for schema, kr in l5_matchings:
            db_name, src_name, attr_name = schema
            kr_name, cla_name = kr
            if (db_name, src_name, attr_name, kr_name, cla_name) in l_combined:
                l_combined[(db_name, src_name, attr_name, kr_name, cla_name)][1].append(MatchingType.L5_CLASSNAME_ATTRNAME_SYN)
            else:
                l_combined[(db_name, src_name, attr_name, kr_name, cla_name)] = ((schema, kr), [MatchingType.L5_CLASSNAME_ATTRNAME_SYN])

        for schema, kr in l52_matchings:
            db_name, src_name, attr_name = schema
            kr_name, cla_name = kr
            if (db_name, src_name, attr_name, kr_name, cla_name) in l_combined:
                l_combined[(db_name, src_name, attr_name, kr_name, cla_name)][1].append(MatchingType.L52_CLASSNAME_ATTRNAME_SEM)
            else:
                l_combined[(db_name, src_name, attr_name, kr_name, cla_name)] = ((schema, kr), [MatchingType.L52_CLASSNAME_ATTRNAME_SEM])

        for schema, kr in l6_matchings:
            db_name, src_name, attr_name = schema
            kr_name, cla_name = kr
            if (db_name, src_name, attr_name, kr_name, cla_name) in l_combined:
                # TODO: only append in the matching types are something except L1?
                l_combined[(db_name, src_name, attr_name, kr_name, cla_name)][1].append(MatchingType.L6_CLASSNAME_RELATION_SEMSIG)

        """
        for key, matching_types in l_combined.items():
            matching, types = matching_types
            running_score = 0
            supported_by_basic_signals = False
            if MatchingType.L2_CLASSVALUE_ATTRVALUE in types:
                running_score += 1
                supported_by_basic_signals = True
            if MatchingType.L5_CLASSNAME_ATTRNAME_SYN in types:
                running_score += 1
                supported_by_basic_signals = True
            elif MatchingType.L52_CLASSNAME_ATTRNAME_SEM in types:
                running_score += 1
                supported_by_basic_signals = True
            if MatchingType.L6_CLASSNAME_RELATION_SEMSIG in types:
                if supported_by_basic_signals:
                    running_score += 1
            combined_matchings.append((matching, running_score))
        """

        # L4 and L42 have their own matching too
        l4_matchings = all_matchings[MatchingType.L4_CLASSNAME_RELATIONNAME_SYN]
        combined_matchings = []
        for key, values in l_combined.items():
            matching = values[0]
            matching_types = values[1]
            #for el in values:
            #    matching = el[0]
            #    matching_types = el[1]
            combined_matchings.append((matching, matching_types))

        return combined_matchings, l4_matchings

    def find_sem_coh_matchings(self):
        matchings = []
        # Get all relations with groups
        table_groups = dict()
        for db, t, attrs in SS.read_table_columns(None, network=self.network):
            groups = SS.extract_cohesive_groups(t, attrs)
            table_groups[(db, t)] = groups

        names = []
        # Retrieve class names
        for kr_name, kr_handler in self.kr_handlers.items():
            all_classes = kr_handler.classes()
            for cl in all_classes:
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
                names.append(('class', (kr_name, cl), svs))
        for db_table_info, groups in table_groups.items():
            db_name, table_name = db_table_info
            class_seen = []  # to filter out already seen classes
            for g_score, g_tokens in groups:
                g_svs = []
                for t in g_tokens:
                    sv = glove_api.get_embedding_for_word(t)
                    if sv is not None:
                        g_svs.append(sv)
                for _, class_info, class_svs in names:
                    kr_name, class_name = class_info
                    sim = SS.compute_semantic_similarity(class_svs, g_svs)
                    if sim > g_score and class_name not in class_seen:
                        class_seen.append(class_name)
                        match = ((db_name, table_name, "_"), (kr_name, class_name))
                        matchings.append(match)
        return matchings

    def find_relation_class_attr_name_sem_matchings(self):
        # Retrieve relation names

        self.find_relation_class_name_sem_matchings()
        st = time.time()
        names = []
        seen_fields = []
        for (db_name, source_name, field_name, _) in self.network.iterate_values():
            orig_field_name = field_name
            if field_name not in seen_fields:
                seen_fields.append(field_name)  # seen already
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
        for kr_name, kr_handler in self.kr_handlers.items():
            all_classes = kr_handler.classes()
            for cl in all_classes:
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
                names.append(('class', (kr_name, cl), svs))

        matchings = []
        for idx_rel in range(0, num_attributes_inserted):  # Compare only with classes
            for idx_class in range(num_attributes_inserted, len(names)):
                svs_rel = names[idx_rel][2]
                svs_cla = names[idx_class][2]
                semantic_sim = SS.compute_semantic_similarity(svs_rel, svs_cla)
                if semantic_sim > 0.8:
                    # match.format db_name, source_name, field_name -> class_name
                    match = ((names[idx_rel][1][0], names[idx_rel][1][1], names[idx_rel][1][2]), names[idx_class][1])
                    matchings.append(match)
        et = time.time()
        print("Time to relation-class (sem): " + str(et - st))
        return matchings

    def find_relation_class_attr_name_matching(self):
        # Retrieve relation names
        st = time.time()
        names = []
        seen_fields = []
        for (db_name, source_name, field_name, _) in self.network.iterate_values():
            orig_field_name = field_name
            if field_name not in seen_fields:
                seen_fields.append(field_name)  # seen already
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
        for kr_name, kr_handler in self.kr_handlers.items():
            all_classes = kr_handler.classes()
            for cl in all_classes:
                cl = nlp.camelcase_to_snakecase(cl)
                cl = cl.replace('-', ' ')
                cl = cl.replace('_', ' ')
                cl = cl.lower()
                m = MinHash(num_perm=64)
                for token in cl.split():
                    if token not in stopwords.words('english'):
                        m.update(token.encode('utf8'))
                names.append(('class', (kr_name, cl), m))

        # Index all the minhashes
        lsh_index = MinHashLSH(threshold=0.3, num_perm=64)

        for idx in range(len(names)):
            lsh_index.insert(idx, names[idx][2])

        matchings = []
        for idx in range(0, num_attributes_inserted):  # Compare only with classes
            N = lsh_index.query(names[idx][2])
            for n in N:
                kind_q = names[idx][0]
                kind_n = names[n][0]
                if kind_n != kind_q:
                    # match.format db_name, source_name, field_name -> class_name
                    match = ((names[idx][1][0], names[idx][1][1], names[idx][1][2]), names[n][1])
                    matchings.append(match)
        return matchings

    def find_relation_class_name_sem_matchings(self):
        # Retrieve relation names
        st = time.time()
        names = []
        seen_sources = []
        for (db_name, source_name, _, _) in self.network.iterate_values():
            if source_name not in seen_sources:
                seen_sources.append(source_name)  # seen already
                source_name = source_name.replace('-', ' ')
                source_name = source_name.replace('_', ' ')
                source_name = source_name.lower()
                svs = []
                for token in source_name.split():
                    if token not in stopwords.words('english'):
                        sv = glove_api.get_embedding_for_word(token)
                        if sv is not None:
                            svs.append(sv)
                names.append(('relation', (db_name, source_name), svs))

        num_relations_inserted = len(names)

        # Retrieve class names
        for kr_name, kr_handler in self.kr_handlers.items():
            all_classes = kr_handler.classes()
            for cl in all_classes:
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
                names.append(('class', (kr_name, cl), svs))

        matchings = []
        for idx_rel in range(0, num_relations_inserted):  # Compare only with classes
            for idx_class in range(num_relations_inserted, len(names)):
                svs_rel = names[idx_rel][2]
                svs_cla = names[idx_class][2]
                semantic_sim = SS.compute_semantic_similarity(svs_rel, svs_cla)
                if semantic_sim > 0.5:
                    # match.format is db_name, source_name, field_name -> class_name
                    match = ((names[idx_rel][1][0], names[idx_rel][1][1], "_"), names[idx_class][1])
                    matchings.append(match)
        et = time.time()
        print("Time to relation-class (sem): " + str(et - st))
        return matchings

    def find_relation_class_name_matchings(self):
        # Retrieve relation names
        st = time.time()
        names = []
        seen_sources = []
        for (db_name, source_name, _, _) in self.network.iterate_values():
            if source_name not in seen_sources:
                seen_sources.append(source_name)  # seen already
                source_name = nlp.camelcase_to_snakecase(source_name)
                source_name = source_name.replace('-', ' ')
                source_name = source_name.replace('_', ' ')
                source_name = source_name.lower()
                m = MinHash(num_perm=32)
                for token in source_name.split():
                    if token not in stopwords.words('english'):
                        m.update(token.encode('utf8'))
                names.append(('relation', (db_name, source_name), m))

        num_relations_inserted = len(names)

        # Retrieve class names
        for kr_name, kr_handler in self.kr_handlers.items():
            all_classes = kr_handler.classes()
            for cl in all_classes:
                cl = nlp.camelcase_to_snakecase(cl)
                cl = cl.replace('-', ' ')
                cl = cl.replace('_', ' ')
                cl = cl.lower()
                m = MinHash(num_perm=32)
                for token in cl.split():
                    if token not in stopwords.words('english'):
                        m.update(token.encode('utf8'))
                names.append(('class', (kr_name, cl), m))

        # Index all the minhashes
        lsh_index = MinHashLSH(threshold=0.3, num_perm=32)

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
        print("Time to relation-class (name): " + str(et-st))
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

    def find_coarse_grain_hooks_n2(self):
        matchings = []
        table_ss = SS.generate_table_vectors(None, network=self.network)  # get semantic signatures of tables
        class_ss = self._get_kr_classes_vectors()
        total = len(class_ss.items())
        idx = 0
        for class_name, class_vectors in class_ss.items():
            print("Checking: " + str(idx) + "/" + str(total) + " : " + str(class_name))
            # for (dbname, table_name), table_vectors in ...
            for db_table, table_vectors in table_ss.items():
                db_name, table_name = db_table
                sim = SS.compute_semantic_similarity(class_vectors, table_vectors)
                print(str(table_name) + " -> " + str(class_name) + " : " + str(sim))
                if sim > 0.85:
                    match = ((db_name, table_name, "_"), class_name)
                    matchings.append(match)
        return matchings

    def _get_kr_classes_vectors(self):
        class_vectors = dict()
        for kr_name, kr in self.kr_handlers.items():
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
                        class_vectors[kr.name_of_class(class_name)] = sem_vectors
                else:
                    print(ret)
        return class_vectors

    def find_coarse_grain_hooks(self):
        """
        Given the model and the parsed KRs, find coarse grain hooks and register them
        :return:
        """

        # TODO: should we do this beforehand?
        # Get ss for each relation in S
        table_ss = SS.generate_table_vectors(self.network)  # get semantic signatures of tables
        ss_key = 0
        for k, v in table_ss.items():
            self.sskey_to_ss[ss_key] = (k, v)  # assign a key to each semantic signature (tablename - sem vectors)
            vkey = 0
            vkeys = []
            for s_vector in v:
                self._lsh_index_vector(s_vector, vkey)  # LSH-index s_vector with key vkey
                self.vkey_to_sskey[vkey] = ss_key  # Keep inverse index from vector to its signature
                vkey += 1
                vkeys.append(vkey)
                self.num_vectors_tables += 1  # One more vector
            self.sskey_to_vkeys[ss_key] = vkeys  # for each signature, keep the list of vector keys it contains
            ss_key += 1

        # Get ss for each class in Cgg
        # TODO:

        # Iterate smaller index (with fewer vectors) and retrieve candidates for coarse-grain hooks
        for sskey, ss in self.sskey_to_ss.items():
            (table_name, semantic_vectors) = ss
            accum = defaultdict(list)
            for s_vector in semantic_vectors:
                neighbors = self._lsh_query(s_vector)
                for (data, key, value) in neighbors:
                    sskey = self.vkey_to_sskey[key]
                    accum[sskey].append(1)
            # We process accum, that contains keys for the ss that had at least one neighbor
            # TODO: we need the new expression here

        return

    def _lsh_index_vector(self, vector, key):
        """
        Index the vector with the associated key
        :param vector:
        :param key:
        :return:
        """
        self.ss_lsh_idx.index(vector, key)

    def _lsh_query(self, vector):
        """
        Query the LSH index
        :param vector:
        :return: (data, key, value)
        """
        n = self.ss_lsh_idx.query(vector)
        return n

    def find_mappings(self):
        """
        Given found coarse grain hooks, perform an in-depth analysis to find mappings
        :return:
        """
        return

    def find_links(self):
        """
        Given existings mappings and parsed KRs, find existing links in the data
        :return:
        """
        return

    def write_semantics(self):
        """
        Push found mappings, links and constraints (properties) to the model
        Reconciliation mechanism goes here (or does it go model side?)
        :return:
        """
        return

    """
    ### OUTPUT FUNCTIONS
    """

    def output_registered_krs(self):
        return

    def output_krs_statistics(self):
        return

    def output_coarse_grain_hooks(self):
        return

    def output_mappings(self):
        return

    def output_links(self):
        return


def test(path_to_serialized_model):
    # Deserialize model
    network = fieldnetwork.deserialize_network(path_to_serialized_model)
    # Create client
    store_client = StoreHandler()

    # Load glove model
    print("Loading language model...")
    path_to_glove_model = "../glove/glove.6B.100d.txt"
    glove_api.load_model(path_to_glove_model)
    print("Loading language model...OK")

    # Retrieve indexes
    schema_sim_index = io.deserialize_object(path_to_serialized_model + 'schema_sim_index.pkl')
    content_sim_index = io.deserialize_object(path_to_serialized_model + 'content_sim_index.pkl')

    # Create ontomatch api
    om = SSAPI(network, store_client, schema_sim_index, content_sim_index)
    # Load parsed ontology
    om.add_krs([("efo", "cache_onto/efo.pkl")], parsed=True)

    matchings = om.find_matchings()
    #matchings = om.find_sem_coh_matchings()

    for m in matchings:
        print(m)
    exit()

    total_matchings = 0
    for k, v in matchings.items():
        total_matchings += len(v)
        print(str(k) + ": " + str(len(v)))
    print("total matchings: " + str(total_matchings))
    print("####")
    for m, v in matchings.items():
        print(m)
        for el in v:
            print(el)

    return om


def main(path_to_serialized_model):
    # Deserialize model
    network = fieldnetwork.deserialize_network(path_to_serialized_model)
    # Create client
    store_client = StoreHandler()

    # Retrieve indexes
    schema_sim_index = io.deserialize_object(path_to_serialized_model + 'schema_sim_index.pkl')
    content_sim_index = io.deserialize_object(path_to_serialized_model + 'content_sim_index.pkl')

    om = SSAPI(network, store_client, schema_sim_index, content_sim_index)

    om.add_krs([("dbpedia", "cache_onto/dbpedia.pkl")], parsed=True)

    return om


def test_find_semantic_sim():
    # Load onto
    om = SSAPI(None, None, None, None)
    # Load parsed ontology
    om.add_krs([("dbpedia", "cache_onto/schemaorg.pkl")], parsed=True)

    # Load glove model
    print("Loading language model...")
    path_to_glove_model = "../glove/glove.6B.100d.txt"
    glove_api.load_model(path_to_glove_model)
    print("Loading language model...OK")

    print("Loading ontology classes...")
    names = []
    # Load classes
    for kr_name, kr_handler in om.kr_handlers.items():
        all_classes = kr_handler.classes()
        for cl in all_classes:
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
            names.append(('class', cl, svs))
    print("Loading ontology classes...OK")

    while True:
        # Get words
        i = input("introduce two words separated by space to get similarity. EXIT to exit")
        tokens = i.split(' ')
        if tokens[0] == "EXIT":
            print("bye!")
            break
        svs = []
        for t in tokens:
            sv = glove_api.get_embedding_for_word(t)
            if sv is not None:
                svs.append(sv)
            else:
                print("No vec for : " + str(t))
        for _, cl, vecs in names:
            sim = SS.compute_semantic_similarity(svs, vecs)
            if sim > 0.4:
                print(str(cl) + " -> " + str(sim))

if __name__ == "__main__":

    #test_find_semantic_sim()
    #exit()

    test("../models/chembl21/")
    exit()

    print("SSAPI")

    path_to_model = ""
    path_to_glove_model = ""
    if len(sys.argv) >= 4:
        path_to_model = sys.argv[2]
        path_to_glove_model = sys.argv[4]

    else:
        print("USAGE")
        print("db: the name of the model to use")
        print("lm: the name of the language model to use")
        exit()

    print("Loading language model...")
    glove_api.load_model(path_to_glove_model)
    print("Loading language model...OK")

    om = main(path_to_model)

    # do things with om now, for example, for testing

