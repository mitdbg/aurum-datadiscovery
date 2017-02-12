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
import pickle
import time
from dataanalysis import dataanalysis as da

# Have a list of accepted formats in the ontology parser


class SSAPI:

    def __init__(self, network, store_client, schema_sim_index, content_sim_index):
        self.network = network
        self.store_client = store_client
        self.schema_sim_index = schema_sim_index
        self.content_sim_index = content_sim_index
        self.srql = API(self.network, self.store_client)
        self.krs = []
        self.kr_handlers = []

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
            self.kr_handlers.append(o)

    def __compare_content_signatures(self, signatures):
        positive_matches = []
        for name, mh_sig in signatures:
            mh_obj = MinHash(num_perm=512)
            mh_array = np.asarray(mh_sig, dtype=int)
            mh_obj.hashvalues = mh_array
            res = self.content_sim_index.query(mh_obj)
            for r_nid in res:
                (nid, db_name, source_name, field_name) = self.network.get_info_for([r_nid])
                matching = (name, (db_name, source_name, field_name))
                positive_matches.append(matching)
        return positive_matches

    def find_matchings(self):
        """
        Find matching for each of the different possible categories
        :return: list of matchings
        """
        # L1: [class] -> attr.content
        print("Finding L1 matchings...")
        kr_class_signatures = []
        for kr_handler in self.kr_handlers:
            kr_class_signatures += kr_handler.get_classes_signatures()

        l1_matchings = self.__compare_content_signatures(kr_class_signatures)
        print("Finding L1 matchings...OK, "+str(len(l1_matchings))+" found")

        for match in l1_matchings:
            print(match)

        # L2: [class.data] -> attr.content
        print("Finding L2 matchings...")
        kr_classdata_signatures = []
        for kr_handler in self.kr_handlers:
            kr_classdata_signatures += kr_handler.get_class_data_signatures()

        l2_matchings = self.__compare_content_signatures(kr_classdata_signatures)
        print("Finding L2 matchings...OK, " + str(len(l2_matchings)) + " found")

        for match in l2_matchings:
            print(match)

        exit()

        # L3: [class.context] -> relation
        print("Finding L3 matchings...")
        l3_matchings = self.find_coarse_grain_hooks_n2()
        print("Finding L3 matchings...OK, " + str(len(l3_matchings)) + " found")

        for match in l3_matchings:
            print(match)

        # L4: [Relation names] -> [Class names]
        print("Finding L4 matchings...")
        l4_matchings = self.find_relation_class_name_matchings()
        print("Finding L4 matchings...OK, " + str(len(l4_matchings)) + " found")

        for match in l4_matchings:
            print(match)

    def find_relation_class_attr_name_matching(self):
        # Retrieve relation names
        st = time.time()
        names = []
        seen_fields = []
        for (_, _, field_name, _) in self.network.iterate_values():
            if field_name not in seen_fields:
                seen_fields.append(field_name)  # seen already
                field_name = field_name.replace('-', ' ')
                field_name = field_name.replace('_', ' ')
                field_name = field_name.lower()
                m = MinHash(num_perm=64)
                for token in field_name.split():
                    m.update(token.encode('utf8'))
                names.append(('attribute', field_name, m))

        num_attributes_inserted = len(names)

        # Retrieve class names
        for kr_handler in self.kr_handlers:
            all_classes = kr_handler.classes()
            for cl in all_classes:
                cl = cl.replace('-', ' ')
                cl = cl.replace('_', ' ')
                cl = cl.lower()
                m = MinHash(num_perm=64)
                for token in cl.split():
                    m.update(token.encode('utf8'))
                names.append(('class', cl, m))

        # Index all the minhashes
        lsh_index = MinHashLSH(threshold=0.1, num_perm=64)

        for idx in range(len(names)):
            lsh_index.insert(idx, names[idx][2])

        matchings = []
        for idx in range(0, num_attributes_inserted):  # Compare only with classes
            N = lsh_index.query(names[idx][2])
            for n in N:
                kind_q = names[idx][0]
                kind_n = names[n][0]
                if kind_n != kind_q:
                    match = names[idx][1], names[n][1]
                    matchings.append(match)
        return matchings

    def find_relation_class_name_sem_matchings(self):
        # Retrieve relation names
        st = time.time()
        names = []
        seen_sources = []
        for (_, source_name, _, _) in self.network.iterate_values():
            if source_name not in seen_sources:
                seen_sources.append(source_name)  # seen already
                source_name = source_name.replace('-', ' ')
                source_name = source_name.replace('_', ' ')
                source_name = source_name.lower()
                svs = []
                for token in source_name.split():
                    sv = glove_api.get_embedding_for_word(token)
                    svs.append(sv)
                names.append(('relation', source_name, svs))

        num_relations_inserted = len(names)

        # Retrieve class names
        for kr_handler in self.kr_handlers:
            all_classes = kr_handler.classes()
            for cl in all_classes:
                cl = cl.replace('-', ' ')
                cl = cl.replace('_', ' ')
                cl = cl.lower()
                svs = []
                for token in cl.split():
                    sv = glove_api.get_embedding_for_word(token)
                    svs.append(sv)
                names.append(('class', cl, svs))

        matchings = []
        for idx_rel in range(0, num_relations_inserted):  # Compare only with classes
            for idx_class in range(num_relations_inserted, len(names)):
                svs_rel = names[idx_rel][2]
                svs_cla = names[idx_class][2]
                semantic_sim = SS.compute_semantic_similarity(svs_rel, svs_cla)
                if semantic_sim > 0.5:
                    match = names[idx_rel][1], names[idx_class][1]
                    matchings.append(match)
        et = time.time()
        print("Time to relation-class (sem): " + str(et - st))
        return matchings

    def find_relation_class_name_matchings(self):
        # Retrieve relation names
        st = time.time()
        names = []
        seen_sources = []
        for (_, source_name, _, _) in self.network.iterate_values():
            if source_name not in seen_sources:
                seen_sources.append(source_name)  # seen already
                source_name = source_name.replace('-', ' ')
                source_name = source_name.replace('_', ' ')
                source_name = source_name.lower()
                m = MinHash(num_perm=32)
                for token in source_name.split():
                    m.update(token.encode('utf8'))
                names.append(('relation', source_name, m))

        num_relations_inserted = len(names)

        # Retrieve class names
        for kr_handler in self.kr_handlers:
            all_classes = kr_handler.classes()
            for cl in all_classes:
                cl = cl.replace('-', ' ')
                cl = cl.replace('_', ' ')
                cl = cl.lower()
                m = MinHash(num_perm=32)
                for token in cl.split():
                    m.update(token.encode('utf8'))
                names.append(('class', cl, m))

        # Index all the minhashes
        lsh_index = MinHashLSH(threshold=0.1, num_perm=32)

        for idx in range(len(names)):
            lsh_index.insert(idx, names[idx][2])

        matchings = []
        for idx in range(0, num_relations_inserted):  # Compare only with classes
            N = lsh_index.query(names[idx][2])
            for n in N:
                kind_q = names[idx][0]
                kind_n = names[n][0]
                if kind_n != kind_q:
                    match = names[idx][1], names[n][1]
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
        for kr_handler in self.kr_handlers:
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
        sim = dict()
        total = len(class_ss.items())
        idx = 0
        for class_name, class_vectors in class_ss.items():
            print("Checking: " + str(idx) + "/" + str(total) + " : " + str(class_name))
            for table_name, table_vectors in table_ss.items():
                sim = SS.compute_semantic_similarity(class_vectors, table_vectors)
                print(str(table_name) + " -> " + str(class_name) + " : " + str(sim))
                if sim > 0.85:
                    match = (class_name, table_name)
                    matchings.append(match)
        return matchings

    def _get_kr_classes_vectors(self):
        class_vectors = dict()
        for kr in self.kr_handlers:
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
    om.add_krs([("uniprotcore", "cache_onto/uniprotcore.pkl")], parsed=True)

    #om.find_coarse_grain_hooks_n2()

    #om.find_matchings()
    #l3_matchings = om.find_coarse_grain_hooks_n2()
    l3_matchings = om.find_relation_class_name_sem_matchings()

    print("Found num matchings: " + str(len(l3_matchings)))

    for match in l3_matchings:
        print(match)

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

    om.add_krs([("efo", "cache_onto/efo/uniprotcore.pkl")], parsed=True)

    return om

if __name__ == "__main__":

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

