import unittest
from knowledgerepr.fieldnetwork import Hit
from api.apiutils import DRS
from ddapi import API


class TestApiutils(unittest.TestCase):

    api = API(None)

    def test_drs_field_iteration(self):
        h1 = Hit(0, "table_a", "a", -1)
        h2 = Hit(1, "table_a", "b", -1)
        h3 = Hit(2, "table_b", "c", -1)
        h4 = Hit(3, "table_b", "d", -1)
        drs = DRS([h1, h2, h3, h4])
        drs.set_fields_mode()

        for el in drs:
            print(str(el))

        self.assertTrue(True)

    def test_drs_table_iteration(self):
        h1 = Hit(0, "table_a", "a", -1)
        h2 = Hit(1, "table_a", "b", -1)
        h3 = Hit(2, "table_b", "c", -1)
        h4 = Hit(3, "table_b", "d", -1)
        drs = DRS([h1, h2, h3, h4])
        drs.set_table_mode()

        for el in drs:
            print(str(el))

        self.assertTrue(True)

if __name__ == "__main__":
    unittest.main()