import numpy
import itertools

def simrank(graph, max_iter, eps, c):
    '''
    Assumes a graph where keys are nodes and values are list of
    nodes to which the key is connected
    '''

    nodes = list(graph.keys())
    nodes_index = {k: v for(k,v) in [(nodes[i], i) for i in range(len(nodes))]}

    sim_prev = numpy.zeros(len(nodes))
    sim = numpy.identity(len(nodes))

    for i in range(max_iter):
        print("It: " + str(i) +"/"+str(max_iter))
        if numpy.allclose(sim, sim_prev, atol=eps):
            break
        sim_prev = numpy.copy(sim)
        for u, v in itertools.product(nodes, nodes):
            if u is v:
                continue
            u_ns, v_ns = predecessor_of(u, graph),predecessor_of(v, graph)
            if len(u_ns) == 0 or len(v_ns) == 0: 
                # if a node has no predecessors then setting similarity to zero
                sim[nodes_index[u]][nodes_index[v]] = 0
            else:                    
                s_uv = sum([sim_prev[nodes_index[u_n]][nodes_index[v_n]] \
                    for u_n, v_n in itertools.product(u_ns, v_ns)])
                sim[nodes_index[u]][nodes_index[v]] = (c * s_uv) / (len(u_ns) * len(v_ns))

    return sim

def predecessor_of(node, graph):
    '''
    Return a list with nodes that point to node
    '''
    l_ns = []
    for k, v in graph.items():
        if k is not node:
            if node in v:
                l_ns.append(k)
    return l_ns

def main():
    from collections import OrderedDict
    test_graph = OrderedDict()
    test_graph['1'] = []
    test_graph['1'].append('2')
    test_graph['1'].append('4')
    test_graph['2'] = []
    test_graph['2'].append('3')
    test_graph['3'] = []
    test_graph['3'].append('1')
    test_graph['4'] = []
    test_graph['4'].append('5')
    test_graph['5'] = []
    test_graph['5'].append('4')
    
    c = 0.8
    eps = 1e-4
    max_iter = 100
    sim = simrank(test_graph, max_iter, eps, c)

    print(test_graph)
    print(sim)
    return (test_graph, sim)

if __name__ == "__main__":
    main()

