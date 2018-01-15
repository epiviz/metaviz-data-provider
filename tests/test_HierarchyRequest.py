"""
   Tests for Hierarchy Request of metaviz data provider
"""

import unittest
from dataprovider.HierarchyRequest import HierarchyRequest
import dataprovider.__init__

class HierarchyRequestTestCase(unittest.TestCase):

    def setUp(self):
        self.param_method = "hierarchy"
        self.request_values = {"params[datasource]": "msd16s",
                               "params[depth]": 3,
                               "params[nodeId]": "0-0",
                               "params[selectedLevels]": u'{"2":2}',
                               "params[selection]": u'{}'
                              }

    def test_hierarchy_request(self):
        metaviz_request = dataprovider.create_request(self.param_method, self.request_values)
        result, errorStr, response_status = metaviz_request.get_data()
        self.assertEqual([1, 2, 9, 18], result['nodesPerLevel'])
        self.assertEqual(1, result['start'])
        self.assertEqual(26044, result['end'])
        self.assertEqual(errorStr, None)
        self.assertEqual(response_status, 200)

if __name__ == '__main__':
    unittest.main()
