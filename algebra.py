from modelstore.elasticstore import StoreHandler

from api.apiutils import Operation
from api.apiutils import OP
from api.apiutils import Scope
from api.apiutils import Relation
from api.apiutils import DRS
from api.apiutils import DRSMode
from api.apiutils import Hit




class Algebra:
    __network = None

    def __init__(self, network, store_client):
        self.__network = network
        self.__store_client = store_client

    """
    Basic API
    """

    def search_keyword(self, kw: str, scope: Relation) -> DRS:
        pass

class API(Algebra):
    def __init__(self, *args, **kwargs):
        super(API, self).__init__(*args, **kwargs)

if __name__ == '__main__':
    print("Aurum API")
