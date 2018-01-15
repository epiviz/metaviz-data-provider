"""
   Tests for Measurements Request of metaviz data provider
"""

import unittest
from dataprovider.MeasurementsRequest import MeasurementsRequest
import dataprovider.__init__

class MeasurementsRequestTestCase(unittest.TestCase):

    def setUp(self):
        self.param_method = "measurements"
        self.request_values = {"params[datasource]": "msd16s"}

    def test_measurements_request(self):
        metaviz_request = dataprovider.create_request(self.param_method, self.request_values)
        result, errorStr, response_status = metaviz_request.get_data()
        self.assertIn("100259", result['id'])
        self.assertEqual(errorStr, None)
        self.assertEqual(response_status, 200)

if __name__ == '__main__':
    unittest.main()