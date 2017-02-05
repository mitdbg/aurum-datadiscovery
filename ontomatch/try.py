#import pronto
#import owlready
import ontospy

owl_file = 'HarryPotter_book.owl'
#owl_file = 'efo.owl'

#def class_iterator(Ontology o):

#def properties_of(Class c)
'''
def class_iterator(Ontology):
    return iter(Ontology)
'''



if __name__ == '__main__':
    ont = ontospy.Ontospy(owl_file)

    print('--- Classes ---')
    print(ont.classes)

    print('--- Properties ---')
    print(ont.properties)


    print('--- Class Tree ---')
    ont.printClassTree

    print('--- Property Tree ---')
    ont.printPropertyTree

    #ont = pronto.Ontology(owl_file)
    #print(ont.terms)
    #print(ont.obo)
    '''
    for term in ont:
        print('------------------------------')
        print(term)
        print(term.relations)
        print(term.children)
    '''
