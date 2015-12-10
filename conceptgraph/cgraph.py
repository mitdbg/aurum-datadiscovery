from dataanalysis import dataanalysis as da

def build_graph_skeleton(columns):
    '''
        Builds basic graph skeleton, where columns
        are connected to other columns in the same table.
        No cross-table connections.
    '''
    graph = dict()
    for p_colname, _ in columns.items():
        (pf, _) = p_colname
        graph[p_colname] = []
        for colname, _ in columns.items():
            (f, c) = colname
            if pf is f:
                if colname not in graph[p_colname]:
                    graph[p_colname].append(colname)
    return graph

def refine_graph_with_columnsignatures(ncol, tcol):
    '''
        Given the signatures for all the numerical columns
        as well as for the textual columns, refine the graph
        creating additional links whenever necessary
    '''
    print("TODO")

def give_neighbors_of(concept, graph):
    '''
        Given a concept it returns all its neighbors not
        in the same table
    '''
    (f, c) = concept
    allneighbors = graph[concept]
    neighbors = [] 
    for filename, col in allneighbors:
        if filename is not f:
            neighbors.append((filename, col)) 
    return neighbors
                 
def give_n_concepts_close_to(num, key, graph):
    return graph[key][:num]
    

if __name__ == "__main__":
    print("TODO")
