"""
   Tests for PCA Request of metaviz data provider
"""

import unittest
from dataprovider.PCARequest import PCARequest
import dataprovider.__init__

class PCARequestTestCase(unittest.TestCase):

    def setUp(self):
        self.param_method = "pca"
        self.request_values = {"params[datasource]": "msd16s",
                               "params[selectedLevels]": u'{"2":2}',
                               "params[measurements]": u'["100259","100262","100267"]'
                              }

    def test_pca_request(self):
        metaviz_request = dataprovider.create_request(self.param_method, self.request_values)
        result, errorStr, response_status = metaviz_request.get_data()
        self.assertEqual(0.35882722429028929, result['data'][2]['PC1'])
        self.assertEqual(0.9109406592226863, result['data'][2]['PC2'])
        self.assertEqual(errorStr, None)
        self.assertEqual(response_status, 200)

if __name__ == '__main__':
    unittest.main()