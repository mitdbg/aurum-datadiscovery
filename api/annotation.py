from collections import namedtuple
from enum import Enum

BaseMDHit = namedtuple(
    'MDHit', 'id, author, md_class, source, ref_target, ref_type, description')


class MDClass(Enum):
    WARNING = 0
    INSIGHT = 1
    QUESTION = 2


class MDRelation(Enum):
    MEANS_SAME_AS = 0
    MEANS_DIFF_FROM = 1
    IS_SUBCLASS_OF = 2
    IS_SUPERCLASS_OF = 3
    IS_MEMBER_OF = 4
    IS_CONTAINER_OF = 5


class MDHit(BaseMDHit):

    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other):
        if isinstance(other, MDHit):
            return self.id == other.id
        elif isinstance(other, str):
            return self.id == other
        return False


class MRS():

    def __init__(self, data):
        self._data = data
        self._idx = 0

    def __iter__(self):
        return self

    def __next__(self):
        if self._idx < len(self._data):
            self._idx += 1
            return self._data[self._idx - 1]
        else:
            self._idx = 0
            raise StopIteration

    @property
    def data(self):
        return self._data

    def set_data(self, data):
        self._data = list(data)
        self._idx = 0
        return self

    def size(self):
        return len(self.data)
