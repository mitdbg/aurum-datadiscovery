from modelstore.elasticstore import StoreHandler

from modelstore.elasticstore import KWType

from api.apiutils import compute_field_id as id_from
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

    def keyword_search(self, kw: str, scope: Scope, max_results=10) -> DRS:
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

    """
    Helper Functions
    """

    def _scope_to_kw_type(self, scope: Scope) -> KWType:
        """
        Converts a relation scope to a keyword type for elasticsearch.
        """
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

    def _node_to_hit(self, node: (str, str, str)) -> DRS:
        """
        Given a field and source name, it returns a Hit with its representation
        :param field: a tuple with the name of the field,
            (db_name, source_name, field_name)
        :return: a Hit
        """
        db, source, field = node
        nid = id_from(db, source, field)
        hit = Hit(nid, db, source, field, 0)
        return hit


class API(Algebra):
    def __init__(self, *args, **kwargs):
        super(API, self).__init__(*args, **kwargs)

if __name__ == '__main__':
    print("Aurum API")
