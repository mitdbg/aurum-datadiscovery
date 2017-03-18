import sys, os
import argparse
import time
from subprocess import call

from elasticsearch import Elasticsearch

class Node:

    def __init__(self, nid, db_name, source_name, column_name,
                 total_values, unique_values, data_type,
                 min_value=0, max_value=0, avg_value=0, median=0, iqr=0):
        self.nid = nid
        self.db_name = db_name
        self.source_name = source_name
        self.column_name = column_name
        self.total_values = int(total_values)
        self.unique_values = int(unique_values)
        self.data_type = data_type
        self.min_value = float(min_value)
        self.max_value = float(max_value)
        self.avg_value = float(avg_value)
        self.median = float(median)
        self.iqr = float(iqr)

    def __eq__(self, obj):
        """
        Two Nodes are equal iff all their fields are equal
        """
        if isinstance(obj, self.__class__):
            return (self.nid == obj.nid and
                self.db_name == obj.db_name and
                self.source_name == obj.source_name and
                self.column_name == obj.column_name and
                self.total_values == obj.total_values and
                self.unique_values == obj.unique_values and
                self.data_type == obj.data_type and
                self.min_value == obj.min_value and
                self.max_value == obj.max_value and
                self.avg_value == obj.avg_value and
                self.median == obj.median and
                self.iqr == obj.iqr)
        return False

    def __ne__(self, other):
        if isinstance(other, self.__class__):
            return not self.__eq__(other)
        return NotImplemented

    def __hash__(self):
        return hash(tuple(sorted(self.__dict__.items())))

    def __str__(self):
        return str(tuple(self.__dict__.values()))

    def __repr__(self):
        return self.__str__()

class TestStoreHandler:

    # Store client
    client = None

    def __init__(self):
        """
        Uses the configuration file to create a connection to the store
        :return:
        """
        global client
        client = Elasticsearch([{'host': 'localhost', 'port': '9200'}])

    def write_all_fields_to_dict(self) -> dict:
        """
        Reads all fields from the store and returns a dictionary mapping
        node ids to their nodes.
        """
        all_fields = {}
        # target = open('%s.dd' % filename, 'w')
        body = {"query": {"match_all": {}}}
        res = client.search(index='profile', body=body, scroll="10m",
                            filter_path=['_scroll_id',
                                         'hits.hits._id',
                                         'hits.total',
                                         'hits.hits._source']
                            )
        scroll_id = res['_scroll_id']
        remaining = res['hits']['total']
        while remaining > 0:
            hits = res['hits']['hits']
            for h in hits:
                # TODO: check minhash
                s = h['_source']
                if s['dataType'] == "N":
                    n = Node(h['_id'],
                            s['dbName'],
                            s['sourceName'],
                            s['columnName'],
                            s['totalValues'],
                            s['uniqueValues'],
                            s['dataType'],
                            s['minValue'],
                            s['maxValue'],
                            s['avgValue'],
                            s['median'],
                            s['iqr'])
                else:
                    n = Node(h['_id'],
                            s['dbName'],
                            s['sourceName'],
                            s['columnName'],
                            s['totalValues'],
                            s['uniqueValues'],
                            s['dataType'])
                all_fields[h['_id']] = n
                # target.write('%s\n' % ','.join(map(str, data)))
                remaining -= 1
            res = client.scroll(scroll="5m", scroll_id=scroll_id,
                                filter_path=['_scroll_id',
                                             'hits.hits._id',
                                             'hits.hits._source']
                                )
            scroll_id = res['_scroll_id']  # update the scroll_id
        client.clear_scroll(scroll_id=scroll_id)
        # target.close()
        return all_fields

    def DELETE_ALL(self):
        """
        Deletes all documents in the elastic store.
        """
        return client.indices.delete(index='_all')

def git_checkout(branch_name: str):
    """
    Checkout branch <branch_name> on github.
    WARNING: stash your changes, or weird things may happen...
    """
    call("git checkout %s" % branch_name, shell=True, stdout=sys.stdout, stderr=sys.stderr)

def build_and_write(branch_name: str, folder_path: str) -> dict:
    """
    1. Build a new ddprofiler jar.
    2. Run the new jar on some files specified by <folder_path>.
    3. Write the store content out to a dictionary.
    """
    # Checkout branch <branch_name>
    git_checkout(branch_name)

    # Clear the elastic store
    print("*** Clearing the elastic store...")
    store_client = TestStoreHandler()
    store_client.DELETE_ALL()

    # Build the jar file
    print("*** Building the jar file...")
    FNULL = open(os.devnull, 'w')
    call("./gradlew clean fatJar", shell=True, stdout=FNULL, stderr=sys.stderr)

    # Write to elastic store
    print("*** Writing to elastic store...")
    print("*** (This may take a while.)")
    jar_cmd = "java -jar build/libs/ddprofiler.jar --execution.mode 1 --sources.folder.path %s" % folder_path
    start = time.time()
    call(jar_cmd, shell=True, stderr=sys.stderr)
    end = time.time()
    print("\nWRITE SUCCESSFUL (%s)\n" % branch_name)
    print("Total time: %.2f secs\n" % (end - start))
    print("*** Finished writing to elastic store!")

    # Write the store content out to a dictionary
    return store_client.write_all_fields_to_dict()

def test_number_of_columns(exp: dict, res: dict):
    """
    exp: expected dict
    res: result dict
    :return: true iff the number of columns are the same
    """
    exp_columns = len(exp)
    res_columns = len(res)
    success = exp_columns == res_columns

    if success:
        print("test_number_of_columns(): OK")
    else:
        print("test_number_of_columns(): ERROR")
        print("Expected %d columns but got %d" % (exp_columns, res_columns))
    return success

def test_statistics(data_type: str, exp: dict, res: dict) -> bool:
    """
    data_type: "N" or "T"
    exp: expected dict
    res: result dict
    :return: true iff the test passes
    """
    num_successes = 0
    missing_error = []  # list of nodes in res but not in exp
    compare_error = []  # (expected node, result node)

    # Check the nodes in res are equal to their corresponding nodes in exp
    for node in res:
        if res[node].data_type != data_type:
            continue

        if node not in exp:
            missing_error.append(res[node].nid)
        elif exp[node] != res[node]:
            compare_error.append((exp[node], res[node]))
        else:
            num_successes += 1

    # Summary
    num_errors = len(missing_error) + len(compare_error)
    if num_errors == 0:
        print("test_statistics() Data Type %s: OK" % data_type)
        return True

    # Oops there's an error
    print("test_statistics() Data Type %s: ERROR" % data_type)
    print()

    # Print information about missing nodes
    if len(missing_error) > 0:
        nodes = ", ".join(map(str, missing_error))
        print("Missing %d nodes: %s" % (len(missing_error), nodes))
        print()

    # Print information about nodes with incorrect statistics
    if len(compare_error) > 0:
        print("Incorrect statistics for %d nodes:" % len(compare_error))
        print()
        for exp, res in compare_error:
            print("Expected: %s" % exp.__str__())
            print("Result: %s\n" % res.__str__())

    return False

def test_numerical_statistics(exp: dict, res: dict):
    pass

if __name__ == "__main__":
    # REQUIREMENTS
    # ./bin/elasticsearch
    # All changes must be stashed!! TODO: make this less dangerous

    parser = argparse.ArgumentParser()
    # no dangerous bash injections please
    parser.add_argument('-p', '--path', metavar='P', required=True, help='path to folder with csv files')
    parser.add_argument('-b1', '--branch_expected', metavar='B2', required=True, help='github branch with expected results')
    parser.add_argument('-b2', '--branch_test', metavar='B2', required=True, help='github branch to analyze')
    inp = parser.parse_args(sys.argv[1:])

    # Write the expected results to a dictionary
    exp: dict = build_and_write(branch_name=inp.branch_expected, folder_path=inp.path)

    # Write the actual results to a dictionary
    res: dict = build_and_write(branch_name=inp.branch_test, folder_path=inp.path)

    # Compare the expected and actual results with each other
    test_number_of_columns(exp=exp, res=res)
    test_statistics(data_type="T", exp=exp, res=res)
    test_statistics(data_type="N", exp=exp, res=res)
