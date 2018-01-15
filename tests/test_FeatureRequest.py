"""
   Tests for Feature Request of metaviz data provider
"""

import unittest
from dataprovider.FeatureRequest import FeatureRequest
import dataprovider.__init__

class FeatureRequestTestCase(unittest.TestCase):

    def setUp(self):
        self.param_method = "featureData"
        self.request_values = {"params[datasource]": "msd16s",
                               "params[selectedLevels]": u'{"2":2}',
                               "params[feature]": u'2-2122f',
                               "params[measurements]": u'["100259","100262","100267"]',
                              }

    def test_combined_request(self):
        metaviz_request = dataprovider.create_request(self.param_method, self.request_values)
        result, errorStr, response_status = metaviz_request.get_data()
        self.assertAlmostEqual(1294.7799385875, result['data'][2]['count'], places=5)
        self.assertEqual(errorStr, None)
        self.assertEqual(response_status, 200)

if __name__ == '__main__':
    unittest.main()

