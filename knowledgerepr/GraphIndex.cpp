//============================================================================
// Name        : GraphIndex.cpp
// Author      : Raul Castro Fernandez
// Version     : 0.1
// Copyright   : Whatever Aurum's license is
// Description : Graph Index Skeleton
// g++ -dynamiclib -o graph_index.so GraphIndex.cpp
//============================================================================

#include <iostream>
#include <sstream>
#include <iterator>
#include <unordered_map>
#include <vector>
#include <chrono>
#include <thread>
#include <string>
#include <new>  // std::nothrow
#include <stdio.h>
#include <iostream>
#include <fstream>
using namespace std;

/***
** Interface
***/


// Global variables
std::unordered_map<int, std::unordered_map<int, char> > g;
int number_edges;

template<typename Out>
void split(const std::string &s, char delim, Out result) {
    std::stringstream ss;
    ss.str(s);
    std::string item;
    while (std::getline(ss, item, delim)) {
        *(result++) = item;
    }
}

vector<string> split(string &s, char delim) {
    vector<string> tokens;
    split(s, delim, back_inserter(tokens));
    return tokens;
}

extern "C" {

    void serialize_graph_to_disk(char* input_path) {
        string path = input_path;
        cout << "Serializing graph to: " + path << endl;
        ofstream f;
        f.open(path);
        for ( auto it = g.begin(); it != g.end(); ++it) {
            int src = it->first;
            unordered_map<int, char> sub = it->second;
            for ( auto it2 = sub.begin(); it2 != sub.end(); ++it2) {
                int tgt = it2->first;
                char type = it2->second;
                string ser = std::to_string(src) + "-" + std::to_string(tgt) + "-" + std::to_string(type) + "\n";
                cout << ser << endl;
                f << ser;
            }
        }
        f.close();
    }

    void deserialize_graph(char* input_path) {
        string path = input_path;
        cout << "Deserializing graph to: " + path << endl;
        string line;
        ifstream f(path);
        if (f.is_open()) {
            while (getline(f, line)) {
                vector<string> tokens = split(line, '-');
                string src = tokens[0];
                string tgt = tokens[1];
                string type = tokens[2];
                cout << src + " . " + tgt + " . " + type << '\n';
                // TODO: transform to int and char first
//                add_node(src);
//                add_node(tgt);
//                add_edge(src, tgt, type);
            }
        }
        f.close();
    }

    int get_num_nodes() {
        return g.size();
    }

    int get_num_edges() {
        return number_edges;
    }

    bool add_node(int id) {
        if (g.count(id) == 1) {
            return false;
        }
        g[id];
        return true;
    }

    bool add_edge(int source_id, int target_id, char type) {
        if (source_id == target_id) {
            return false;
        }
        add_node(source_id);
        add_node(target_id);
        if (g[source_id].count(target_id) == 0) {
            g[source_id][target_id] = type;
            number_edges++;
        }
        else if ((g[source_id][target_id] | type) == 0) {
            g[source_id][target_id] |= type;
            number_edges++;
        }
        return true;
    }

    bool add_undirected_edge(int source_id, int target_id, char type) {
        add_edge(source_id, target_id, type);
        add_edge(target_id, source_id, type);
        return true;
    }

    int neighbors(int32_t** output, int id, char type) {
        vector<int> n;
        unordered_map<int, char> nodes_map = g[id];
        for ( auto it = nodes_map.begin(); it != nodes_map.end(); ++it) {
            if ((it->second & type) == 1) {
                n.push_back(it->first);
            }
        }

        // copy to array
        int32_t* array = (int32_t*) malloc(n.size() * sizeof(int32_t));
        //int array[n.size()];
        for (int i = 0; i < n.size(); i++) {
            int id = n[i];
            cout << id << endl;
            array[i] = id;
        }

        *output = array;
        cout << n.size() << endl;
        return n.size();
    }

    void release_array(int32_t *input) {
        free(input);
    }

}


/***
** OO Interface
***/

class GraphIndex {
public:

    int test_add_one(int i) {
        return i + 1;
    }

    int get_num_nodes() {
        return g.size();
    }

    int get_num_edges() {
        return number_edges;
    }

    bool add_node(int id) {
        if (g.count(id) == 1) {
            return false;
        }
        g[id];
        return true;
    }

    bool add_edge(int source_id, int target_id, char type) {
        if (source_id == target_id) {
            return false;
        }
        this->add_node(source_id);
        this->add_node(target_id);
        if (g[source_id].count(target_id) == 0) {
            g[source_id][target_id] = type;
            number_edges++;
        }
        else if ((g[source_id][target_id] | type) == 0) {
            g[source_id][target_id] |= type;
            number_edges++;
        }
        return true;
    }

    bool add_undirected_edge(int source_id, int target_id, char type) {
        this->add_edge(source_id, target_id, type);
        this->add_edge(target_id, source_id, type);
        return true;
    }

    vector<int> neighbors(int id, char type) {
        vector<int> n;
        unordered_map<int, char> nodes_map = g[id];
        for ( auto it = nodes_map.begin(); it != nodes_map.end(); ++it) {
            if ((it->second & type) == 1) {
                n.push_back(it->first);
            }
        }
        return n;
    }

private:
    std::unordered_map<int, std::unordered_map<int, char> > g;
    int number_edges;
};

/**
** Some basic testing
**/

// Testing iface with Python

extern "C" void myprint(void);

extern "C" void myprint() {

    printf("hellow world from graph index\n");
}

extern "C" int add_one(int i) {
    return i + 1;
}

int main() {

    class GraphIndex g;

    g.add_node(0);
    g.add_node(1);
    g.add_node(2);
    g.add_edge(0, 1, 33);
    int nn = g.get_num_nodes();
    int e = g.get_num_edges();

    vector<int> neighbors = g.neighbors(0, 33);

    for (auto it = neighbors.begin(); it != neighbors.end(); ++it) {
        cout << *it << endl;
    }

    cout << nn << endl;
    cout << e << endl;


    class GraphIndex lite;

    int num_nodes = 10000;

    for (int i = 0; i < num_nodes; i++) {
        g.add_node(i);
    }

    for (int i = 0; i < num_nodes; i++) {
        for (int j = 0; j < num_nodes; j++) {
            g.add_edge(i, j, 1);
        }
    }

    for (int i = 0; i < num_nodes; i++) {
        for (int j = 0; j < num_nodes; j++) {
            g.add_edge(i, j, 2);
        }
    }

    int numnodes = g.get_num_nodes();
    int numedges = g.get_num_edges();

    cout << numnodes << endl;
    cout << numedges << endl;

    std::this_thread::sleep_for(std::chrono::seconds(10));
    cout << "done" << endl;

}

