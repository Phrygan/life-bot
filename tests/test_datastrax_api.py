from datastrax_api import DataStraxApi as dsa

from datetime import datetime

import unittest

class TestDatastraxApi(unittest.TestCase):

    def test_itemid(self):
        self.assertEqual(dsa.itemid(datetime(2021, 1, 1), 'food'), '1-2021:food')

    def test_parse_itemid(self):
        self.assertEqual(dsa.parse_itemid('1-2021:food'), (datetime(2021, 1, 1), 'food'))

if __name__ == '__main__':
    unittest.main()