import ontospy
import time
import pickle
import sys
import rdflib
from dataanalysis import nlp_utils as nlp
from ontomatch.ss_utils import minhash


# We are serializing highly nested structures here...
sys.setrecursionlimit(50000)


class OntoHandler:

    def __init__(self):
        self.ontology_name = None
        self.o = None
        self.objectProperties = []
        self.class_hierarchy = []

    def parse_ontology(self, file):
        """
        Preprocess ontology into library-specific format
        :param file:
        :return:
        """
        self.ontology_name = file
        ont = ontospy.Ontospy(file)
        self.o = ont
        self.objectProperties = self.o.objectProperties  # cache this
        self.class_hierarchy = self.__get_class_levels_hierarchy()  # preprocess this

    def store_ontology(self, path):
        """
        Serialize processed ontology to avoid pre-processing time
        :param ont:
        :param path:
        :return:
        """
        f = open(path, 'wb')
        pickle.dump(self.o, f)
        f.close()

    def load_ontology(self, path):
        """
        Load processed ontology
        :param path:
        :return:
        """
        f = open(path, 'rb')
        self.o = pickle.load(f)
        self.objectProperties = self.o.objectProperties
        self.class_hierarchy = self.__get_class_levels_hierarchy()  # pre_load this

    def classes(self):
        """
        Return list of classes
        :param o:
        :return:
        """
        return [x.bestLabel().title() for x in self.o.classes]

    def classes_id(self):
        """
        Return list of IDs
        :return:
        """
        return [x.id for x in self.o.classes]

    def name_of_class(self, class_id):
        """
        Returns name of a class given its id
        :param class_id:
        :return:
        """
        c = self.o.getClass(id=class_id)
        name = c.bestLabel().title()  # title to avoid rdflib types
        return name

    def id_of_class(self, class_name):
        """
        Returns class id given its name, or -1 if no class named that way
        :param class_name:
        :return:
        """
        c = self.o.getClass(match=class_name)[0]
        if len(c) > 0:
            cid = c.id
            return cid
        else:
            return -1

    def __get_class_levels_hierarchy(self, element=None):
        if not element:
            levels = [[top.id for top in self.o.toplayer]]
            for x in self.o.toplayer:
                levels.extend(self.__get_class_levels_hierarchy(x))
            return levels

        if not element.children():
            return []

        levels = [[child.id for child in element.children()]]
        for sub in element.children():
            levels.extend(self.__get_class_levels_hierarchy(sub))
        return levels

    def class_levels_count(self):
        """
        Return the number of levels in the class hierarchy. This is equivalent to nodes in a tree.
        :return:
        """
        return len(self.class_hierarchy)

    def class_hierarchy_iterator(self, class_id=False):
        """
        Returns lists of classes that are at the same level of the hierarhcy, i.e., node in a tree
        :return:
        """
        if class_id:
            return self.class_hierarchy
        else:
            # (level_id, [classes in that level])
            class_levels = []
            for i in range(len(self.class_hierarchy)):
                level = self.class_hierarchy[i]
                if len(level) > 5:
                    class_levels.append((i, [self.name_of_class(c) for c in level]))

        return class_levels

    def get_classes_signatures(self):
        """
        Return a minhash signature of the class-names per class hierarchy level
        :return:
        """
        class_hierarchy_signatures = []
        class_hierarchies = self.class_hierarchy_iterator()
        for level_name, ch in class_hierarchies:
            mh = minhash(ch)
            chs = (level_name, mh)
            class_hierarchy_signatures.append(chs)
        return class_hierarchy_signatures

    def parents_of_class(self, class_name, class_id=False):
        """
        Parents of given class
        :param class_name:
        :return:
        """
        if class_id:
            return self.o.getClass(id=class_name).parents()
        return self.o.getClass(match=class_name)[0].parents()

    def children_of_class(self, class_name, class_id=False):
        """
        Children of given class
        :param class_name:
        :return:
        """
        if class_id:
            return self.o.getClass(id=class_name).children()
        return self.o.getClass(match=class_name)[0].children()

    def properties_all_of(self, class_name, class_id=False):
        """
        All properties associated to the given class (both datatype and object)
        :param class_name:
        :return:
        """
        if class_id:
            c = self.o.getClass(id=class_name)
        else:
            c = self.o.getClass(match=class_name)[0]
        properties = self.o.getInferredPropertiesForClass(c)
        props = []
        for k, v in properties.items():
            for el in v:
                props.append(el)
        return props

    def get_class_data_signatures(self):
        signatures = []
        for cid in self.classes_id():
            data = self.instances_of(cid, class_id=True)
            sig = minhash(data)
            class_name = self.name_of_class(cid)
            signatures.append((class_name, sig))
        return signatures

    def instances_of(self, class_name, class_id=False):
        """
        When data is available, retrieve all data for the given class
        :param c:
        :param p:
        :return:
        """
        if class_id:
            c = self.o.getClass(id=class_name)
        else:
            c = self.o.getClass(match=class_name)[0]
        clean_data = []
        data = c.instances()
        for d in data:
            if type(d) == rdflib.term.URIRef:
                d = d.title()
            clean_data.append(d)
        return clean_data

    def relations_of(self, class_name, class_id=False):
        """
        Return for the given class all properties that refere to other classes, i.e., they are relations
        :param c:
        :return:
        """
        if class_id:
            c = self.o.getClass(id=class_name)
        else:
            c = self.o.getClass(match=class_name)[0]
        property_for_class = self.o.getInferredPropertiesForClass(c)[:1]
        properties = []
        for dic in property_for_class:
            for k, v in dic.items():
                if len(v) > 0:
                    properties += v
        relations = []  # hosting (label, descr)
        for p in properties:
            if p in self.objectProperties:
                label = p.bestLabel()
                descr = p.bestDescription()
                relations.append((label, descr))
        return relations

    def bow_repr_of(self, class_name, class_id=False):
        """
        Retrieve a bag of words (bow) representation of the given class
        :param c:
        :return: (boolean, (class_name, bow)) if a bow can be built, or (boolean, str:reason) if not
        """
        if class_id:
            c = self.o.getClass(id=class_name)
        else:
            c = self.o.getClass(match=class_name)[0]
            if c is not None:
                c = c[0]
        if c is None:
            return False, "Class does not exist"  # means there's no

        label = c.bestLabel()
        descr = c.bestDescription()
        # Get class name, description -> bow, properties -> bow
        pnouns = nlp.get_proper_nouns(descr)
        nouns = nlp.get_nouns(descr)
        bow_descr = pnouns + nouns
        props = self.relations_of(class_name, class_id=class_id)
        bow_properties = []
        for prop_label, prop_descr, in props:
            tokens = nlp.tokenize_property(prop_label)
            bow_properties.extend(tokens)
        bow = bow_descr + bow_properties
        bow = nlp.curate_tokens(bow)
        ret = (label, bow)
        return True, ret


if __name__ == '__main__':


    #owl_file = '/Users/ra-mit/data/uniprot/uniprotcore.owl'
    #owl_file = 'opencyc.owl'
    #owl_file = 'efo.owl'
    """
    owl_file = "/home/jian/EKG/dbpedia_2016-04.owl"
    o = OntoHandler()

    s = time.time()
    o.parse_ontology(owl_file)
    e = time.time()
    print("Parse: " + str(e - s))


    #o.store_ontology("cache_onto/opencyc.pkl")
    #exit()

    o.store_ontology("cache_onto/dbpedia.pkl")
    exit()
    """

    s = time.time()
    file = "cache_onto/dbpedia.pkl"

    o.load_ontology(file)
    e = time.time()
    print("Load: " + str(e - s))

    for cl in o.classes():
        cl = nlp.camelcase_to_snakecase(cl)
        print(cl)

    """
    print("classes")
    for c in o.classes_id():
        name = o.name_of_class(c)
        print(name)
        data = o.instances_of(c, class_id=True)

        if len(data) > 0:
            print(data)

        print("Gonna get bow for: " + str(c))
        s, bow = o.bow_repr_of(c, class_id=True)
        if s:
            if len(bow[1]) > 0:
                print(bow)
    """

    #stats = o.o.stats()
    #for name, value in stats:
    #    print(str(name) + " -> " + str(value))
