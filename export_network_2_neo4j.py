from knowledgerepr import fieldnetwork
from knowledgerepr.ekgstore.neo4j_store import *
from api.apiutils import Relation
import sys

if __name__=="__main__":
    path=None
    neodb=None
    if len(sys.argv)==5:
        path=sys.argv[2]
        neo_url=sys.argv[4]
    else:
        print('USAGE:')
        print("python export_network_2_neo4j.py --opath <path> --neo_url neo4j_server")
        sys.exit(1)
    serialize_network_to_neo4j(path,neo_url)
