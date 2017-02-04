import ontospy


#owl_file = 'HarryPotter_book.owl'
owl_file = 'efo.owl'

def class_iterator(o):
    return iter(o.classes)

def unit_iterator(o):
    return iter(o.properties)

def properties_of(c):
    return c.triples

def instance_of(c, p = None):
    pass

def relations_of(c):
    super_classes = [('super_class', x) for x in c.parents()]
    sub_classes = [('sub_class', x) for x in c.children()]
    return super_classes + sub_classes


def plain_text_repr_of(c):
    pass

if __name__ == '__main__':
    ont = ontospy.Ontospy(owl_file)

    for c in class_iterator(ont):
        print('-------------------------')
        print(c)
        print(relations_of(c))
        print(properties_of(c))

    for u in unit_iterator(ont):
        print(u)


"""
    print('--- Classes ---')
    print(ont.classes)

    print('--- Properties ---')
    print(ont.properties)


    print('--- Class Tree ---')
    ont.printClassTree

    print('--- Property Tree ---')
    ont.printPropertyTree
"""
