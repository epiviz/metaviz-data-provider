"""
   Tests for Search Request of metaviz data provider
"""

import unittest
from dataprovider.SearchRequest import SearchRequest
import dataprovider.__init__

class SearchRequestTestCase(unittest.TestCase):

    def setUp(self):
        self.param_method = "search"
        self.request_values = {"params[datasource]": "msd16s",
                               "params[maxResults]": u'12',
                               "params[searchQuery]": "Fi"
                              }

    def test_pca_request(self):
        metaviz_request = dataprovider.create_request(self.param_method, self.request_values)
        result, errorStr, response_status = metaviz_request.get_data()
        self.assertEqual(7215, result[0]['start'])
        self.assertEqual(16660, result[0]['end'])
        self.assertEqual('phylum', result[0]['level'])
        self.assertEqual(errorStr, None)
        self.assertEqual(response_status, 200)

if __name__ == '__main__':
    unittest.main()