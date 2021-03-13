from unittest.mock import patch
from freezegun import freeze_time

import unittest
import util


class TestUtil(unittest.TestCase):
    @freeze_time('2021-01-01')
    @patch('builtins.print')
    def test_log_event(self, mock_print):
        util.log_event('hello', module='connected_database')
        mock_print.assert_called_with('[connected_database | 00:00:00] hello')
        util.log_event('hello')
        mock_print.assert_called_with('[00:00:00] hello')

if __name__ == '__main__':
    unittest.main()