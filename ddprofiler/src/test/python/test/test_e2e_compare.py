import sys, os
import argparse
import time
from subprocess import call

from elasticsearch import Elasticsearch

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

    def write_all_fields_to_csv(self, filename: str):
        """
        Reads all fields from the store and writes them to a csv.
        """
        target = open('%s.dd' % filename, 'w')
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
                    data = [h['_id'],
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
                            s['iqr']]
                else:
                    data = [h['_id'],
                            s['dbName'],
                            s['sourceName'],
                            s['columnName'],
                            s['totalValues'],
                            s['uniqueValues'],
                            s['dataType'],
                            0, 0, 0, 0, 0]
                target.write('%s\n' % ','.join(map(str, data)))
                remaining -= 1
            res = client.scroll(scroll="5m", scroll_id=scroll_id,
                                filter_path=['_scroll_id',
                                             'hits.hits._id',
                                             'hits.hits._source']
                                )
            scroll_id = res['_scroll_id']  # update the scroll_id
        client.clear_scroll(scroll_id=scroll_id)
        target.close()

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

def build_and_write(branch_name: str, folder_path: str):
    """
    1. Build a new ddprofiler jar.
    2. Run the new jar on some files specified by <folder_path>.
    3. Write the store content out to a <branch_name>.dd file.
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
    call(jar_cmd, stdout=FNULL, shell=True, stderr=sys.stderr)
    end = time.time()
    print("\nWRITE SUCCESSFUL (%s)\n" % branch_name)
    print("Total time: %.2f secs\n" % (end - start))
    print("*** Finished writing to elastic store!")

    # Write the store content out to a <branch_name>.dd file
    print("*** Writing the store content out to %s.dd..." % branch_name)
    store_client.write_all_fields_to_csv("src/test/python/test/%s" % branch_name)


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

    # Write the expected results to <inp.branch_expected>.dd
    build_and_write(branch_name=inp.branch_expected, folder_path=inp.path)

    # Write the actual results to <inp.branch_test>.dd
    build_and_write(branch_name=inp.branch_test, folder_path=inp.path)
