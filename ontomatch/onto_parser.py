import ontospy
import time
import pickle
import sys
import rdflib
from dataanalysis import nlp_utils as nlp

# We are serializing highly nested structures here...
sys.setrecursionlimit(10000)


class OntoHandler:

    def __init__(self):
        self.ontology_name = None
        self.o = None
        self.objectProperties = []

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

    def classes(self):
        """
        Iterate over classes
        :param o:
        :return:
        """
        return [x.bestLabel() for x in self.o.classes]

    def parents_of_class(self, class_name):
        """
        Parents of given class
        :param class_name:
        :return:
        """
        return self.o.getClass(match=class_name)[0].parents()

    def children_of_class(self, class_name):
        """
        Children of given class
        :param class_name:
        :return:
        """
        return self.o.getClass(match=class_name)[0].children()

    def properties_all_of(self, class_name):
        """
        All properties associated to the given class (both datatype and object)
        :param class_name:
        :return:
        """
        c = self.o.getClass(match=class_name)[0]
        properties = self.o.getInferredPropertiesForClass(c)
        props = []
        for k, v in properties.items():
            for el in v:
                props.append(el)
        return props

    def instances_of(self, class_name):
        """
        When data is available, retrieve all data for the given class
        :param c:
        :param p:
        :return:
        """
        c = self.o.getClass(match=class_name)[0]
        clean_data = []
        data = c.instances()
        for d in data:
            if type(d) == rdflib.term.URIRef:
                d = d.title()
            clean_data.append(d)
        return clean_data

    def relations_of(self, class_name):
        """
        Return for the given class all properties that refere to other classes, i.e., they are relations
        :param c:
        :return:
        """
        c = self.o.getClass(match=class_name)[0]
        property_for_class = self.o.getInferredPropertiesForClass(c)
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

    def bow_repr_of(self, class_name):
        """
        Retrieve a bag of words (bow) representation of the given class
        :param c:
        :return: (boolean, (class_name, bow)) if a bow can be built, or (boolean, str:reason) if not
        """
        c = self.o.getClass(match=class_name)
        if c is None:
            return False, "Class does not exist"  # means there's no
        c = c[0]
        label = c.bestLabel()
        descr = c.bestDescription()
        # Get class name, description -> bow, properties -> bow
        pnouns = nlp.get_proper_nouns(descr)
        nouns = nlp.get_nouns(descr)
        bow_descr = pnouns + nouns
        props = self.relations_of(class_name)
        bow_properties = []
        for prop_label, prop_descr, in props:
            tokens = nlp.tokenize_property(prop_label)
            bow_properties.extend(tokens)
        bow = bow_descr + bow_properties
        bow = nlp.curate_tokens(bow)
        ret = (label, bow)
        return True, ret


if __name__ == '__main__':

    owl_file = 'HarryPotter_book.owl'
    #owl_file = 'efo.owl'

    s = time.time()
    o = OntoHandler()
    #o.parse_ontology(owl_file)
    e = time.time()
    print("Parse: " + str(e - s))

    #o.store_ontology("test.test")

    s = time.time()
    file = "test.test"
    o.load_ontology(file)
    e = time.time()
    print("Load: " + str(e - s))

    print("classes")
    for c in o.classes():
        print("Gonna get bow for: " + str(c))
        bow = o.bow_repr_of(c)
        print(bow)
