from enum import Enum


class DRSMode(Enum):
    FIELDS = 0
    TABLE = 1


class DRS:

    def __init__(self, data):
        self._data = data
        self._idx = 0
        self._mode = DRSMode.FIELDS

    def __iter__(self):
        return self

    def __next__(self):
        """
        for el in self.data:
            yield el
        """
        if self._idx < len(self._data):
            self._idx += 1
            return self._data[self._idx - 1]
        else:
            raise StopIteration

    @property
    def data(self):
        return self._data

    @property
    def mode(self):
        return self._mode

    def set_fields_mode(self):
        self._mode = DRSMode.FIELDS

    def set_table_mode(self):
        self._mode = DRSMode.TABLE


if __name__ == "__main__":

    test = DRS([1, 2, 3])

    for el in test:
        print(str(el))

    print(test.mode)
    test.set_table_mode()
    print(test.mode)
