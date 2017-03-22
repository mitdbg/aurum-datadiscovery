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
from ontomatch import matcher_lib as matcherlib
from ontomatch.matcher_lib import MatchingType
import operator

# Have a list of accepted formats in the ontology parser


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

    def compare_content_signatures(self, kr_name, signatures):
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

    def priv_build_content_sim(self, threshold):
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
        all_matchings = defaultdict(list)

        # Build content sim
        self.priv_build_content_sim(0.6)

        # L1: [class] -> attr.content
        st = time.time()
        print("Finding L1 matchings...")
        kr_class_signatures = []
        l1_matchings = []
        for kr_name, kr_handler in self.kr_handlers.items():
            kr_class_signatures = kr_handler.get_classes_signatures()
            l1_matchings += self.compare_content_signatures(kr_name, kr_class_signatures)

        print("Finding L1 matchings...OK, "+str(len(l1_matchings))+" found")
        et = time.time()
        print("Took: " + str(et-st))
        all_matchings[MatchingType.L1_CLASSNAME_ATTRVALUE] = l1_matchings

        #for match in l1_matchings:
        #    print(match)

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
        #l3_matchings = matcherlib.find_relation_class_sem_coh_clss_context(self.network, self.kr_handlers)
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
        l4_matchings = matcherlib.find_relation_class_name_matchings(self.network, self.kr_handlers)
        print("Finding L4 matchings...OK, " + str(len(l4_matchings)) + " found")
        et = time.time()
        print("Took: " + str(et - st))
        all_matchings[MatchingType.L4_CLASSNAME_RELATIONNAME_SYN] = l4_matchings

        # L4.2: [Relation names] -> [Class names] (semantic)
        print("Finding L42 matchings...")
        st = time.time()
        l42_matchings, neg_l42_matchings = matcherlib.find_relation_class_name_sem_matchings(self.network, self.kr_handlers,
                                                                                             sem_sim_threshold=0.5,
                                                                                             sensitivity_neg_signal=0.4)
        print("Finding L42 matchings...OK, " + str(len(l42_matchings)) + " found")
        et = time.time()
        print("Took: " + str(et - st))
        all_matchings[MatchingType.L42_CLASSNAME_RELATIONNAME_SEM] = l42_matchings

        print("Does L42 cancel any L4?")
        print("Original L4: " + str(len(all_matchings[MatchingType.L4_CLASSNAME_RELATIONNAME_SYN])))
        cancelled_l4_matchings = []
        l4_dict = dict()
        l4_matchings_set = set(l4_matchings)
        for matching in l4_matchings_set:
            l4_dict[matching] = 1
        total_cancelled = 0
        for m in neg_l42_matchings:
            if m in l4_dict:
                total_cancelled += 1
                l4_matchings_set.remove(m)
                cancelled_l4_matchings.append(m)
        l4_matchings = list(l4_matchings_set)
        all_matchings[MatchingType.L4_CLASSNAME_RELATIONNAME_SYN] = l4_matchings  # update with corrections

        print("Cancelled: " + str(total_cancelled))
        print("Resulting L4: " + str(len(all_matchings[MatchingType.L4_CLASSNAME_RELATIONNAME_SYN])))

        #for match in l42_matchings:
        #    print(match)

        # L5: [Attribute names] -> [Class names] (syntax)
        print("Finding L5 matchings...")
        st = time.time()
        l5_matchings = matcherlib.find_relation_class_attr_name_matching(self.network, self.kr_handlers)
        print("Finding L5 matchings...OK, " + str(len(l5_matchings)) + " found")
        et = time.time()
        print("Took: " + str(et - st))
        all_matchings[MatchingType.L5_CLASSNAME_ATTRNAME_SYN] = l5_matchings

        #for match in l5_matchings:
        #    print(match)

        #l52_matchings = []
        # L52: [Attribute names] -> [Class names] (semantic)
        print("Finding L52 matchings...")
        st = time.time()
        l52_matchings, neg_l52_matchings = matcherlib.find_relation_class_attr_name_sem_matchings(self.network, self.kr_handlers,
                                                                               semantic_sim_threshold=0.7,
                                                                               sensitivity_neg_signal=0.4)
        print("Finding L52 matchings...OK, " + str(len(l52_matchings)) + " found")
        et = time.time()
        print("Took: " + str(et - st))
        all_matchings[MatchingType.L52_CLASSNAME_ATTRNAME_SEM] = l52_matchings

        print("Does L52 cancel any L5?")
        print("Original L5: " + str(len(all_matchings[MatchingType.L5_CLASSNAME_ATTRNAME_SYN])))
        total_neg = len(neg_l52_matchings)
        cancelled_l5_matchings = []
        l5_dict = dict()
        for matching in l5_matchings:
            l5_dict[matching] = 1
        total_cancelled = 0
        for m in neg_l52_matchings:
            if m in l5_dict:
                total_cancelled += 1
                l5_matchings.remove(m)
                cancelled_l5_matchings.append(m)
        print("Cancelled: " + str(total_cancelled))
        all_matchings[MatchingType.L5_CLASSNAME_ATTRNAME_SYN] = l5_matchings
        print("Resulting L5: " + str(len(all_matchings[MatchingType.L5_CLASSNAME_ATTRNAME_SYN])))

        ## L6: [Relations] -> [Class names] (semantic groups)
        #print("Finding L6 matchings...")
        #st = time.time()
        #l6_matchings, table_groups = matcherlib.find_sem_coh_matchings(self.network, self.kr_handlers,
        #                                                               sem_sim_threshold=0.5)
        #print("Finding L6 matchings...OK, " + str(len(l6_matchings)) + " found")
        #et = time.time()
        #print("Took: " + str(et - st))
        l6_matchings = []
        all_matchings[MatchingType.L6_CLASSNAME_RELATION_SEMSIG] = l6_matchings

        # L7: [Attribute names] -> [class names] (content - fuzzy naming)
        print("Finding L7 matchings...")
        st = time.time()
        l7_matchings = matcherlib.find_hierarchy_content_fuzzy(self.kr_handlers, self.store_client)
        print("Finding L7 matchings...OK, " + str(len(l7_matchings)) + " found")
        et = time.time()
        print("Took: " + str(et - st))

        all_matchings[MatchingType.L7_CLASSNAME_ATTRNAME_FUZZY] = l7_matchings

        total_matchings_pre_combined = 0
        for values in all_matchings.values():
            total_matchings_pre_combined += len(values)
        print("ALL_matchings: " + str(total_matchings_pre_combined))

        combined_matchings = matcherlib.combine_matchings(all_matchings)
        print("COMBINED_matchings: " + str(len(combined_matchings.items())))

        return combined_matchings

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
                sim, strong_signal = SS.compute_semantic_similarity(class_vectors, table_vectors)
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

    def find_links(self, matchings):
        """
        Given existings matchings and parsed KRs, find existing links in the data
        :return: a list of found links. Each element in the list is a tuple with 3 components:
        ((el1), relation_name, (el2))
        Each element (el1 and el2) are a locator of the attribute/relation involved in the link
        """

        # There are two kinds of links we discover, those between attributes, and those between relations

        # Iterate over matchings
        # For each matching, take ontology class (2nd element of the tuple), and take its hierarchy in the ontology

        # is any other element in the ontology involved in a matching?
        # YES -> create a link (is_a) between the elements in the schema (reverse mapping the onto class)
        # is any other element in the properties involved in a matching? (object property in OWL)
        # YES -> create a link (objectProperty name) between the elements in the schema

        # NOTES:
        # matchings always point from an element int he schema to a class in an ontology
        # for this function to work efficiently, probably one wants to create a map from onto class to schema element
        # note that the links are between elements of the schema (no ontologies involved here)

        # build the mapping: (kr, class name) -> class
        # build a set of object properties pointing to at least one class

        #print(matchings)

        map_ontoclass_name_to_class = dict()
        set_object_properties = set()

        for kr_name, o in self.kr_handlers.items():
            for c in o.o.classes:
                c_name = c.bestLabel().title()
                map_ontoclass_name_to_class[(kr_name, c_name)] = c

            for p in o.o.objectProperties:
                if p.ranges:
                    set_object_properties.add((kr_name, p))
                

        # build the mapping: (kr, class) -> schema
        map_ontoclass_to_schema = dict()
        
        for matching, matching_types in matchings:
            schema, kr = matching
            kr_name, cla_name = kr
            
            o = self.kr_handlers[kr_name]
            #print(cla_name)
            #onto_class = o.o.getClass(match=cla_name)[0]
            onto_class = map_ontoclass_name_to_class[(kr_name, cla_name)]
            #print(kr_name, cla_name)
            #print(onto_class)
            if onto_class in map_ontoclass_to_schema:
                map_ontoclass_to_schema[onto_class].append(schema)
            else:
                map_ontoclass_to_schema[onto_class] = [schema]

        links = set()

        print("finding all links...")
        # find all links
        for matching, matching_types in matchings:
            schema_A, kr = matching
            kr_name, cla_name = kr

            o = self.kr_handlers[kr_name]
            
            #onto_class_A = o.o.getClass(match=cla_name)
            onto_class_A = map_ontoclass_name_to_class[(kr_name, cla_name)]
            # find is_a links using hierarchy of ancestors and descendants
            #for onto_class_B in [onto_class_A] + o.ancestors_of_class(onto_class_A) + o.descendants_of_class(onto_class_A):
            for onto_class_B in [onto_class_A] + o.ancestors_of_class(onto_class_A): # asymmetric of is_a relationship
                if onto_class_B in map_ontoclass_to_schema:
                    schemas = map_ontoclass_to_schema[onto_class_B]
                    for schema_B in schemas:
                        if schema_B != schema_A:
                            links.add((schema_A, "is_a", schema_B))
            # find property links
            properties = o.get_properties_all_of(onto_class_A)
            for p in properties:
                if (kr_name, p) in set_object_properties:
                    for onto_class_B in p.ranges:
                        if onto_class_B in map_ontoclass_to_schema:
                            schemas = map_ontoclass_to_schema[onto_class_B]
                            for schema_B in schemas:
                                if schema_B != schema_A:
                                    links.add((schema_A, (p.bestLabel().title(), repr(p)), schema_B))
                
        return list(links)

    def find_coarse_grain_hooks(self):
        # FIXME: deprecated?
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
        # FIXME: deprecated?
        """
        Index the vector with the associated key
        :param vector:
        :param key:
        :return:
        """
        self.ss_lsh_idx.index(vector, key)

    def _lsh_query(self, vector):
        # FIXME: deprecated?
        """
        Query the LSH index
        :param vector:
        :return: (data, key, value)
        """
        n = self.ss_lsh_idx.query(vector)
        return n

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


def test_l6(path_to_serialized_model):
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
    om.add_krs([("clo", "cache_onto/clo.pkl")], parsed=True)
    om.add_krs([("bao", "cache_onto/bao.pkl")], parsed=True)
    om.add_krs([("go", "cache_onto/go.pkl")], parsed=True)  # parse again

    # L6: [Relations] -> [Class names] (semantic groups)
    print("Finding L6 matchings...")
    st = time.time()
    l6_matchings, table_groups, neg_l6_matchings = matcherlib.find_sem_coh_matchings(om.network, om.kr_handlers,
                                                                   sem_sim_threshold=0.5)
    print("Finding L6 matchings...OK, " + str(len(l6_matchings)) + " found")
    et = time.time()
    print("Took: " + str(et - st))

    print("coh groups")
    for k, v in table_groups.items():
        print(str(k))
        print(str(v))

    print("matchings")
    for m in l6_matchings:
        print(m)


def test_e2e(path_to_serialized_model):
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
    om.add_krs([("clo", "cache_onto/clo.pkl")], parsed=True)
    om.add_krs([("bao", "cache_onto/bao.pkl")], parsed=True)
    om.add_krs([("go", "cache_onto/go.pkl")], parsed=True)  # parse again
    #om.add_krs([("dbpedia", "cache_onto/dbpedia.pkl")], parsed=True)

    print("Finding matchings...")
    st = time.time()
    matchings = om.find_matchings()
    et = time.time()
    print("Finding matchings...OK")
    print("Took: " + str(et-st))

    print("Writing MATCHINGS output to disk...")
    with open('MATCHINGS_OUTPUT_MASSDATA', 'w') as f:
        for k, v in matchings.items():
            lines = v.print_serial()
            for l in lines:
                f.write(l + '\n')
    print("Writing MATCHINGS output to disk...OK")

    matchings = []
    line = 0
    with open("MATCHINGS_OUTPUT_MASSDATA", 'r') as f:
        lines = f.readlines()
        for l in lines:
            tokens = l.split("==>>")
            sch = tokens[0]
            cla = tokens[1]
            sch_tokens = sch.split("%%%")
            sch_tokens = [t.strip() for t in sch_tokens]
            cla_tokens = cla.split("%%%")
            cla_tokens = [t.strip() for t in cla_tokens]
            matching_format = (((sch_tokens[0], sch_tokens[1], sch_tokens[2]), (cla_tokens[0], cla_tokens[1])), cla_tokens[2])
            matchings.append(matching_format)
            print(line)
            line += 1

    print("Finding links...")
    st = time.time()
    links = om.find_links(matchings)
    et = time.time()
    print("Finding links...OK")
    print("Took: " + str((et-st)))

    print("Writing LINKS output to disk...")
    with open('LINKS_OUTPUT_MASSDATA', 'w') as f:
        for l in links:
            f.write(str(l) + '\n')
    print("Writing LINKS output to disk...OK")

    for link in links:
        print(link)

    return om


def generate_matchings(input_model_path, input_ontology_name_path, output_file):
    # Deserialize model
    network = fieldnetwork.deserialize_network(input_model_path)
    # Create client
    store_client = StoreHandler()

    # Load glove model
    print("Loading language model...")
    path_to_glove_model = "../glove/glove.6B.100d.txt"
    glove_api.load_model(path_to_glove_model)
    print("Loading language model...OK")

    # Retrieve indexes
    schema_sim_index = io.deserialize_object(input_model_path + 'schema_sim_index.pkl')
    content_sim_index = io.deserialize_object(input_model_path + 'content_sim_index.pkl')

    # Create ontomatch api
    om = SSAPI(network, store_client, schema_sim_index, content_sim_index)
    for onto_name, onto_parsed_path in input_ontology_name_path:
        # Load parsed ontology
        om.add_krs([(onto_name, onto_parsed_path)], parsed=True)

    matchings = om.find_matchings()

    with open(output_file, 'w') as f:
        for m in matchings:
            f.write(str(m) + '\n')

    print("Done!")


def test_4_n_42(path_to_serialized_model):
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
    om.add_krs([("clo", "cache_onto/clo.pkl")], parsed=True)
    #om.add_krs([("bao", "cache_onto/bao.pkl")], parsed=True)
    #om.add_krs([("go", "cache_onto/go.pkl")], parsed=True)


    print("Finding matchings...")
    st = time.time()
    # L4: [Relation names] -> [Class names] (syntax)
    print("Finding L4 matchings...")
    st = time.time()
    l4_matchings = matcherlib.find_relation_class_name_matchings(om.network, om.kr_handlers)
    print("Finding L4 matchings...OK, " + str(len(l4_matchings)) + " found")
    et = time.time()
    print("Took: " + str(et - st))

    print("computing fanout L4")
    fanout = defaultdict(int)
    for m in l4_matchings:
        sch, cla = m
        fanout[sch] += 1
    ordered = sorted(fanout.items(), key=operator.itemgetter(1), reverse=True)
    for o in ordered:
        print(o)

    # L4.2: [Relation names] -> [Class names] (semantic)
    print("Finding L42 matchings...")
    st = time.time()
    l42_matchings, neg_l42_matchings = matcherlib.find_relation_class_name_sem_matchings(om.network, om.kr_handlers)
    print("Finding L42 matchings...OK, " + str(len(l42_matchings)) + " found")
    et = time.time()
    print("Took: " + str(et - st))
    et = time.time()
    print("Finding matchings...OK")
    print("Took: " + str(et - st))

    print("computing fanout L42")
    fanout = defaultdict(int)
    for m in l42_matchings:
        sch, cla = m
        fanout[sch] += 1
    ordered = sorted(fanout.items(), key=operator.itemgetter(1), reverse=True)
    for o in ordered:
        print(o)

    print("are l4 subsumed by l42?")
    not_in_l42 = 0
    not_subsumed = []
    for m in l4_matchings:
        if m not in l42_matchings:
            not_in_l42 += 1
            not_subsumed.append(m)
    print("NOT-subsumed: " + str(not_in_l42))

    print("does l42 cancel any l4?")
    total_neg = len(neg_l42_matchings)
    cancelled_matchings = []
    l4_dict = dict()
    for matching in l4_matchings:
        l4_dict[matching] = 1
    total_cancelled = 0
    test = 0
    for m in neg_l42_matchings:
        if m in l4_dict:
            total_cancelled += 1
            cancelled_matchings.append(m)
            print(m)

    print("Total l4: " + str(len(l4_matchings)))
    print("Total l42: " + str(len(l42_matchings)))
    print("Total not_subsumed: " + str(len(not_subsumed)))
    print("Total neg_l42: " + str(total_neg))
    print("Total cancelled out: " + str(total_cancelled))



    """
    # L5: [Attribute names] -> [Class names] (syntax)
    print("Finding L5 matchings...")
    st = time.time()
    l5_matchings = matcherlib.find_relation_class_attr_name_matching(om.network, om.kr_handlers)
    print("Finding L5 matchings...OK, " + str(len(l5_matchings)) + " found")
    et = time.time()
    print("Took: " + str(et - st))

    # for match in l5_matchings:
    #    print(match)

    # l52_matchings = []

    # L52: [Attribute names] -> [Class names] (semantic)
    print("Finding L52 matchings...")
    st = time.time()
    l52_matchings = matcherlib.find_relation_class_attr_name_sem_matchings(om.network, om.kr_handlers)
    print("Finding L52 matchings...OK, " + str(len(l52_matchings)) + " found")
    et = time.time()
    print("Took: " + str(et - st))

    """

    with open('OUTPUT_442_only', 'w') as f:
        f.write("L4" + '\n')
        for m in l4_matchings:
            f.write(str(m) + '\n')
        f.write("L42" + '\n')
        for m in l42_matchings:
            f.write(str(m) + '\n')
        #f.write("L5" + '\n')
        #for m in l5_matchings:
        #    f.write(str(m) + '\n')
        #f.write("L52" + '\n')
        #for m in l52_matchings:
        #    f.write(str(m) + '\n')
        f.write("l4 not subsubmed by l42" + '\n')
        for m in not_subsumed:
            f.write(str(m) + '\n')

        f.write("l42 cancels the following l4" + '\n')
        for m in cancelled_matchings:
            f.write(str(m) + '\n')


def test_5_n_52(path_to_serialized_model):
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
    om.add_krs([("clo", "cache_onto/clo.pkl")], parsed=True)
    om.add_krs([("bao", "cache_onto/bao.pkl")], parsed=True)
    om.add_krs([("go", "cache_onto/go.pkl")], parsed=True)
    #om.add_krs([("dbpedia", "cache_onto/dbpedia.pkl")], parsed=True)  # parse again

    print("Finding matchings...")
    st = time.time()
    # L4: [Relation names] -> [Class names] (syntax)
    print("Finding L5 matchings...")
    st = time.time()
    l5_matchings = matcherlib.find_relation_class_attr_name_matching(om.network, om.kr_handlers)
    print("Finding L5 matchings...OK, " + str(len(l5_matchings)) + " found")
    et = time.time()
    print("Took: " + str(et - st))

    print("computing fanout L5")
    fanout = defaultdict(int)
    for m in l5_matchings:
        sch, cla = m
        fanout[sch] += 1
    ordered = sorted(fanout.items(), key=operator.itemgetter(1), reverse=True)
    for o in ordered:
        print(o)

    # L5.2: [Relation names] -> [Class names] (semantic)
    print("Finding L52 matchings...")
    st = time.time()
    l52_matchings, neg_l52_matchings = matcherlib.find_relation_class_attr_name_sem_matchings(
        om.network,
        om.kr_handlers,
        semantic_sim_threshold=0.6,
        sensitivity_neg_signal=0.7)
    print("Finding L52 matchings...OK, " + str(len(l52_matchings)) + " found")
    et = time.time()
    print("Took: " + str(et - st))
    et = time.time()
    print("Finding matchings...OK")
    print("Took: " + str(et - st))

    print("computing fanout L52")
    fanout = defaultdict(int)
    for m in l52_matchings:
        sch, cla = m
        fanout[sch] += 1
    ordered = sorted(fanout.items(), key=operator.itemgetter(1), reverse=True)
    for o in ordered:
        print(o)

    print("are l5 subsumed by l52?")
    not_in_l52 = 0
    not_subsumed = []
    for m in l5_matchings:
        if m not in l52_matchings:
            not_in_l52 += 1
            not_subsumed.append(m)
    print("NOT-subsumed: " + str(not_in_l52))

    print("does l52 cancel any l5?")
    total_neg = len(neg_l52_matchings)
    cancelled_matchings = []
    l5_dict = dict()
    for matching in l5_matchings:
        l5_dict[matching] = 1
    total_cancelled = 0
    for m in neg_l52_matchings:
        if m in l5_dict:
            total_cancelled += 1
            cancelled_matchings.append(m)
            print(m)

    print("Total l5: " + str(len(l5_matchings)))
    print("Total l52: " + str(len(l52_matchings)))
    print("Total not_subsumed: " + str(len(not_subsumed)))
    print("Total neg_l52: " + str(total_neg))
    print("Total cancelled out: " + str(total_cancelled))

    with open('OUTPUT_552_only', 'w') as f:
        f.write("L5" + '\n')
        for m in l5_matchings:
            f.write(str(m) + '\n')
        f.write("L52" + '\n')
        for m in l52_matchings:
            f.write(str(m) + '\n')

        f.write("l5 not subsubmed by l52" + '\n')
        for m in not_subsumed:
            f.write(str(m) + '\n')

        f.write("l52 cancels the following l5" + '\n')
        for m in cancelled_matchings:
            f.write(str(m) + '\n')


def can_l6_cancel_l42_and_l52(path_to_serialized_model):
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
    #om.add_krs([("clo", "cache_onto/clo.pkl")], parsed=True)
    #om.add_krs([("bao", "cache_onto/bao.pkl")], parsed=True)
    #om.add_krs([("go", "cache_onto/go.pkl")], parsed=True)
    # om.add_krs([("dbpedia", "cache_onto/dbpedia.pkl")], parsed=True)  # parse again

    # L5.2: [Relation names] -> [Class names] (semantic)
    print("Finding L52 matchings...")
    st = time.time()
    l52_matchings, neg_l52_matchings = matcherlib.find_relation_class_attr_name_sem_matchings(
        om.network,
        om.kr_handlers,
        semantic_sim_threshold=0.7,
        sensitivity_neg_signal=0.4)
    print("Finding L52 matchings...OK, " + str(len(l52_matchings)) + " found")
    et = time.time()
    print("Took: " + str(et - st))
    et = time.time()
    print("Finding matchings...OK")
    print("Took: " + str(et - st))

    # L4.2: [Relation names] -> [Class names] (semantic)
    print("Finding L42 matchings...")
    st = time.time()
    l42_matchings, neg_l42_matchings = matcherlib.find_relation_class_name_sem_matchings(om.network, om.kr_handlers)
    print("Finding L42 matchings...OK, " + str(len(l42_matchings)) + " found")
    et = time.time()
    print("Took: " + str(et - st))
    et = time.time()
    print("Finding matchings...OK")
    print("Took: " + str(et - st))

    # L6: [Relations] -> [Class names] (semantic groups)
    print("Finding L6 matchings...")
    st = time.time()
    l6_matchings, table_groups = matcherlib.find_sem_coh_matchings(om.network, om.kr_handlers,
                                                                   sem_sim_threshold=0.5,
                                                                   group_size_cutoff=1)
    print("Finding L6 matchings...OK, " + str(len(l6_matchings)) + " found")
    et = time.time()
    print("Took: " + str(et - st))

    total_l42 = len(l42_matchings)
    total_l52 = len(l52_matchings)
    total_l6 = len(l6_matchings)

    # prepare lookup structures
    l42_dict = dict()
    for matching in l42_matchings:
        l42_dict[matching] = 1

    l52_dict = dict()
    for matching in l52_matchings:
        # adapt matching to be compared to L6
        sch, cla = matching
        sch0, sch1, sch2 = sch
        matching = ((sch0, sch1, '_'), cla)
        l52_dict[matching] = 1

    l6_dict = dict()
    for matching in l6_matchings:
        l6_dict[matching] = 1

    print("l42 AND l6")
    l42_and_l6 = []
    for m in l42_matchings:
        if m in l6_dict:
            l42_and_l6.append(m)

    print("l42 and not l6")
    l42_and_not_l6 = []
    for m in l42_matchings:
        if m not in l6_dict:
            l42_matchings.remove(m)
            l42_and_not_l6.append(m)
    total_l42_after_correction = len(l42_matchings)

    print("l6 and not l42")
    l6_and_not_l42 = []
    for m in l6_matchings:
        if m not in l42_dict:
            l6_and_not_l42.append(m)

    print("l52 AND l6")
    l52_and_l6 = []
    for m in l52_dict.keys():
        if m in l6_dict:
            l52_and_l6.append(m)

    print("l52 and not l6")
    l52_and_not_l6 = []
    for m in l52_dict.keys():
        if m not in l6_dict:
            db_to_remove, rel_to_remove, _ = m[0]
            for el in l52_matchings:
                sch, cla = el
                db, relation, attr = sch
                if db == db_to_remove and relation == rel_to_remove:
                    if el in l52_matchings:
                        l52_matchings.remove(el)
                        l52_and_not_l6.append(el)
    total_l52_after_correction = len(l52_matchings)

    print("l6 and not l52")
    l6_and_not_l52 = []
    for m in l6_matchings:
        if m not in l52_dict:
            l6_and_not_l52.append(m)

    print("Statistics:")
    print("L6: " + str(total_l6))
    print("L42: " + str(total_l42))
    print("L52: " + str(total_l52))
    print("L6 AND L42: " + str(len(l42_and_l6)))
    print("L6 and NOT L42: " + str(len(l6_and_not_l42)))
    print("L42 and NOT L6: " + str(len(l42_and_not_l6)))
    print("L6 and L52: " + str(len(l52_and_l6)))
    print("L6 and NOT L52: " + str(len(l6_and_not_l52)))
    print("L52 and NOT L6: " + str(len(l52_and_not_l6)))
    print("L42 after cancellation: " + str(total_l42_after_correction))
    print("L52 after cancellation: " + str(total_l52_after_correction))

    with open('UNDERSTANDING_L6', 'w') as f:
        f.write("L6" + '\n')
        for m in l6_matchings:
            f.write(str(m) + '\n')
        f.write("L42" + '\n')
        for m in l42_matchings:
            f.write(str(m) + '\n')
        f.write("L52" + '\n')
        for m in l52_matchings:
            f.write(str(m) + '\n')
        f.write("L6 AND L42" + '\n')
        for m in l42_and_l6:
            f.write(str(m) + '\n')
        f.write("L6 AND NOT L42" + '\n')
        for m in l6_and_not_l42:
            f.write(str(m) + '\n')
        f.write("L42 AND NOT L6" + '\n')
        for m in l42_and_not_l6:
            f.write(str(m) + '\n')
        f.write("L6 AND L52" + '\n')
        for m in l52_and_l6:
            f.write(str(m) + '\n')
        f.write("L6 AND NOT L52" + '\n')
        for m in l6_and_not_l52:
            f.write(str(m) + '\n')
        f.write("L52 AND NOT L6" + '\n')
        for m in l52_and_not_l6:
            f.write(str(m) + '\n')


def main(path_to_serialized_model):
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

    om = SSAPI(network, store_client, schema_sim_index, content_sim_index)

    om.add_krs([("dbpedia", "cache_onto/dbpedia.pkl")], parsed=True)

    matchings = om.find_matchings()

    print("Found: " + str(len(matchings)))
    for m in matchings:
        print(m)

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
            sim, strong_signal = SS.compute_semantic_similarity(svs, vecs)
            if sim > 0.4:
                print(str(cl) + " -> " + str(sim))


def test_fuzzy(path_to_serialized_model):
    # Deserialize model
    network = fieldnetwork.deserialize_network(path_to_serialized_model)
    # Create client
    store_client = StoreHandler()

    # Retrieve indexes
    schema_sim_index = io.deserialize_object(path_to_serialized_model + 'schema_sim_index.pkl')
    content_sim_index = io.deserialize_object(path_to_serialized_model + 'content_sim_index.pkl')

    # Create ontomatch api
    om = SSAPI(network, store_client, schema_sim_index, content_sim_index)
    # Load parsed ontology
    om.add_krs([("efo", "cache_onto/efo.pkl")], parsed=True)

    matchings = matcherlib.find_hierarchy_content_fuzzy(om.kr_handlers, store_client)

    for m in matchings:
        print(m)


def test_find_links(path_to_serialized_model, matchings):
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

    om = SSAPI(network, store_client, schema_sim_index, content_sim_index)

    #om.add_krs([("efo", "cache_onto/efo.pkl")], parsed=True)
    #om.add_krs([("clo", "cache_onto/clo.pkl")], parsed=True)
    #om.add_krs([("bao", "cache_onto/bao.pkl")], parsed=True)
    om.add_krs([("dbpedia", "cache_onto/dbpedia.pkl")], parsed=True)

    links = om.find_links(matchings)
    for link in links:
        print(link)


def compute_coh_groups(path_to_serialized_model):
    # Deserialize model
    network = fieldnetwork.deserialize_network(path_to_serialized_model)

    # Create client
    store_client = StoreHandler()

    # Load glove model
    print("Loading language model...")
    path_to_glove_model = "../glove/glove.6B.100d.txt"
    glove_api.load_model(path_to_glove_model)
    print("Loading language model...OK")

    table_groups = dict()
    for db, t, attrs in SS.read_table_columns(None, network=network):
        groups = SS.extract_cohesive_groups(t, attrs, sem_sim_threshold=0.5)
        table_groups[(db, t)] = groups  # (score, [set()])

    for k, v in table_groups.items():
        print(str(k) + " -> " + str(v))


def print_table_attrs_for(path_to_serialized_model):
    # Deserialize model
    network = fieldnetwork.deserialize_network(path_to_serialized_model)
    sch_elements = []
    for db, t, attrs in SS.read_table_columns(None, network=network):
        for attr in attrs:
            sch_el = db + " %%% " + t + " %%% " + attr + " ==>> "
            print(str(sch_el))
            sch_elements.append(sch_el)

    with open('SCH_ELEMENTS', 'w') as f:
        for m in sch_elements:
            f.write(str(m) + '\n')
    print("DONE")


def test_chembl_annotations(path_to_serialized_model):
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
    om.add_krs([("clo", "cache_onto/clo.pkl")], parsed=True)
    om.add_krs([("bao", "cache_onto/bao.pkl")], parsed=True)
    om.add_krs([("uberon", "cache_onto/uberon.pkl")], parsed=True)  # parse again
    #om.add_krs([("go", "cache_onto/go.pkl")], parsed=True)  # parse again
    # om.add_krs([("dbpedia", "cache_onto/dbpedia.pkl")], parsed=True)

    print("Finding matchings...")
    st = time.time()
    combined_matchings = om.find_matchings()

    def list_from_dict(combined):
        l = []
        for k, v in combined.items():
            matchings = v.get_matchings()
            for el in matchings:
                l.append(el)
        return l

    matchings = list_from_dict(combined_matchings)

    matchings = matcherlib.summarize_matchings_to_ancestor(om, matchings)
    et = time.time()
    print("Finding matchings...OK")
    print("Took: " + str(et - st))

    print("Writing MATCHINGS output to disk...")
    with open('MATCHINGS_chembl_annotations', 'w') as f:
        for k, v in matchings.items():
            lines = v.print_serial()
            for l in lines:
                f.write(l + '\n')
    print("Writing MATCHINGS output to disk...OK")


if __name__ == "__main__":

    #test_find_semantic_sim()
    #exit()

    #test_fuzzy("../models/chembl21/")
    #exit()

    #compute_coh_groups("../models/chembl22/")
    #exit()

    #test_5_n_52("../models/chembl22/")
    #exit()

    test_chembl_annotations("../models/chembl22/")
    exit()

    print_table_attrs_for("../models/chembl22/")
    exit()

    can_l6_cancel_l42_and_l52("../models/chembl22/")
    exit()

    test_l6("../models/chembl22/")
    exit()

    test_e2e("../models/chembl22/")
    exit()

    #test("../models/massdata/")
    #exit()

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

