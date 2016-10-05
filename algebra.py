from modelstore.elasticstore import StoreHandler

from modelstore.elasticstore import KWType

from api.apiutils import Operation
from api.apiutils import OP
from api.apiutils import Scope
from api.apiutils import Relation
from api.apiutils import DRS
from api.apiutils import DRSMode
from api.apiutils import Hit




class Algebra:

    def __init__(self, network, store_client):
        self._network = network
        self._store_client = store_client

    """
    Basic API
    """

    def search_keyword(self, kw: str, scope: KWType, max_results=10) -> DRS:
        import pdb; pdb.set_trace()
        hits = self.__store_client.search_keyword(
            kw, scope, max_results=max_results)

        pass

class API(Algebra):
    def __init__(self, *args, **kwargs):
        super(API, self).__init__(*args, **kwargs)

if __name__ == '__main__':
    print("Aurum API")
