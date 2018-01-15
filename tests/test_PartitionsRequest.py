"""
   Tests for Partitions Request of metaviz data provider
"""

import unittest
from dataprovider.PartitionsRequest import PartitionsRequest
import dataprovider.__init__

class PartitionsRequestTestCase(unittest.TestCase):

    def setUp(self):
        self.param_method = "partitions"
        self.request_values = {"params[datasource]": "msd16s"}

    def test_partitions_request(self):
        metaviz_request = dataprovider.create_request(self.param_method, self.request_values)
        result, errorStr, response_status = metaviz_request.get_data()
        self.assertIn(26044, result[0])
        self.assertEqual(errorStr, None)
        self.assertEqual(response_status, 200)

if __name__ == '__main__':
    unittest.main()