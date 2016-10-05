from modelstore.elasticstore import StoreHandler

from api.apiutils import Operation
from api.apiutils import OP
from api.apiutils import Relation
from api.apiutils import DRS
from api.apiutils import DRSMode
from api.apiutils import Hit



class Algebra:
    __network = None

    def __init__(self, network):
        self.__network = network
        self.__store_client = StoreHandler()

    """
    Basic API
    """

    # def keyword_search(self, kw: str, max_results=10) -> DRS:
    #     pass

    def stupid_method(self):
        print(self.__store_client)
        pass

class API(Algebra):
    def __init__(self, *args, **kwargs):
        super(API, self).__init__(*args, **kwargs)

if __name__ == '__main__':
    print("Aurum API")
