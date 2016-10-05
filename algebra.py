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

    def search_keyword(self, kw: str, scope: Scope, max_results=10) -> DRS:
        """
        Performs a keyword search over the contents of the data.
        Scope specifies where elasticsearch should be looking for matches.
        i.e. table titles (SOURCE), columns (FIELD), or comment (SOURCE)

        :param kw: the keyword to serch
        :param max_results: maximum number of results to return
        :return: returns a DRS
        """

        kw_type = self._scope_to_kw_type(scope)
        hits = self._store_client.search_keyword(
            keywords=kw, elasticfieldname=kw_type, max_results=max_results)

        # materialize generator
        drs = DRS([x for x in hits], Operation(OP.KW_LOOKUP, params=[kw]))
        return drs


    def _scope_to_kw_type(self, scope: Scope) -> KWType:
        kw_type = None
        if scope == Scope.DB:
            raise Error('DB Scope is not implemeneted')
            # # raise Exception('spam', 'eggs')
            # # raise(NameError())
            # raise ValueError('The day is too frabjous.')
        elif scope == Scope.SOURCE:
            kw_type = KWType.KW_TABLE
        elif scope == Scope.FIELD:
            kw_type = KWType.KW_SCHEMA
        elif scope == Scope.CONTENT:
            kw_type = KWType.KW_TEXT

        return kw_type


class API(Algebra):
    def __init__(self, *args, **kwargs):
        super(API, self).__init__(*args, **kwargs)

if __name__ == '__main__':
    print("Aurum API")
