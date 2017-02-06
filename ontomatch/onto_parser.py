import ontospy
import time
import pickle
import sys

# We are serializing highly nested structures here...
sys.setrecursionlimit(10000)


class OntoHandler:

    def __init__(self):
        self.ontology_name = None
        self.o = None

    def parse_ontology(self, file):
        """
        Preprocess ontology into library-specific format
        :param file:
        :return:
        """
        self.ontology_name = file
        ont = ontospy.Ontospy(file)
        self.o = ont

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

    def classes(self):
        """
        Iterate over classes
        :param o:
        :return:
        """
        return self.o.classes

    def parents_of_class(self, class_name):
        # Parents of that class
        return self.o.getClass(class_name)

    def children_of_class(self, class_name):
        # TODO:
        return

    def properties_of(self, class_name):
        # TODO:
        return

    def instances_of(self, c, p = None):
        # TODO: any individuals/instances/data associated to the class
        return

    def relations_of(self, c):
        # TODO: properties that refer to relations (objectProperties in owl terminology)
        return

    def plain_text_repr_of(self, c):
        # TODO: for a given class return (class_name, class_description, properties)
        pass

if __name__ == '__main__':

    owl_file = 'HarryPotter_book.owl'
    #owl_file = 'efo.owl'

    s = time.time()
    o = OntoHandler()
    o.parse_ontology(owl_file)
    e = time.time()
    print("Parse: " + str(e - s))

    o.store_ontology("test.test")

    s = time.time()
    file = "test.test"
    o.load_ontology(file)
    e = time.time()
    print("Load: " + str(e - s))

    print("classes")
    for o in o.classes():
        print(str(o))
