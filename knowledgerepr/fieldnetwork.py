import networkx as nx

# The core graph
G = nx.MultiGraph()


def compute_field_id(source_name, field_name):
    def java_hash_code(string):
        str_len = len(string)
        h = 0  # will keep the hash
        for char in string:
            for i in range(str_len):
                h = 31 * h + ord(char)
        return h
    string = source_name + field_name
    nid = java_hash_code(string)
    return nid


class Node:

    __nid = None
    __source_name = None
    __field_name = None

    def __init__(self, source_name, field_name):
        self.__nid = compute_field_id(source_name, field_name)
        self.__source_name = source_name
        self.__field_name = field_name

    @property
    def nid(self):
        return self.__nid

    @property
    def field_name(self):
        return self.__field_name

    @property
    def source_name(self):
        return self.__source_name

    def __str__(self):
        return self.__source_name + " - " + self.__field_name

    def __hash__(self):
        return self.__nid

    def __eq__(self, y):
        if self.__nid == y.__nid:
            return True
        return False

if __name__ == "__main__":
    print("Field Network")
    node1 = Node("source1", "field1")
    node2 = Node("source1", "field2")
    node3 = Node("source1", "field1")

    assert node1 != node2
    assert node2 != node3
    n1 = hash(node1)
    n2 = hash(node2)
    n3 = hash(node3)
    assert node1 == node3
    print("all good")

