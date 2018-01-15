"""
   Tests for PCoA Request of metaviz data provider
"""

import unittest
from dataprovider.PCoARequest import PCoARequest
import dataprovider.__init__

class PCoARequestTestCase(unittest.TestCase):

    def setUp(self):
        self.param_method = "pcoa"
        self.request_values = {"params[datasource]": "msd16s",
                               "params[selectedLevels]": u'{"2":2}',
                               "params[measurements]": u'["100259","100262","100267"]'
                              }

    def test_pcoa_request(self):
        metaviz_request = dataprovider.create_request(self.param_method, self.request_values)
        result, errorStr, response_status = metaviz_request.get_data()
        self.assertEqual(0.44010128713507007, result['data'][2]['PC1'])
        self.assertEqual(0.1507398405074819, result['data'][2]['PC2'])
        self.assertEqual(errorStr, None)
        self.assertEqual(response_status, 200)

if __name__ == '__main__':
    unittest.main()
