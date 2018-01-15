"""
   Tests for Combined Request of metaviz data provider
   Referenced the following resources during development:
   http://flask.pocoo.org/docs/0.12/testing/
   https://docs.python.org/2/library/unittest.html
   https://github.com/pallets/flask/blob/master/examples/flaskr/tests/test_flaskr.py
"""

import unittest
import math
from dataprovider.CombinedRequest import CombinedRequest
import dataprovider.__init__

class CombinedRequestTestCase(unittest.TestCase):

    def setUp(self):
        self.param_method = "combined"
        self.request_values = {"params[datasource]": "msd16s",
                               "params[selectedLevels]": u'{"2":2}',
                               "params[selection]": u'{}',
                               "params[start]": u'1',
                               "params[end]": u'26044',
                               "params[measurements]": u'["100259","100262","100267"]',
                               "params[order]": {},
                               "params[partition]": "msd16s"
                              }

    def test_combined_request(self):
        metaviz_request = dataprovider.create_request(self.param_method, self.request_values)
        result, errorStr, response_status = metaviz_request.get_data()
        print(result)
        print(errorStr)
        #Results derived from metagenomeSeq aggeratedByTaxonomy
        self.assertAlmostEqual(39.18919, result['cols']['100259'][0], places=3)
        self.assertAlmostEqual(1871.62162, result['cols']['100259'][1], places=3)
        self.assertAlmostEqual(1287.83784, result['cols']['100259'][2], places=3)
        self.assertAlmostEqual(12.16216, result['cols']['100259'][3], places=3)
        self.assertAlmostEqual(205.40541, result['cols']['100259'][4], places=3)
        self.assertAlmostEqual(0.00000, result['cols']['100259'][5])
        self.assertAlmostEqual(2679.72973, result['cols']['100259'][6], places=3)

        self.assertAlmostEqual(28.659161, result['cols']['100262'][0], places=3)
        self.assertAlmostEqual(1446.264074, result['cols']['100262'][1], places=3)
        self.assertAlmostEqual(1294.779939, result['cols']['100262'][2], places=3)
        self.assertAlmostEqual(9.211873, result['cols']['100262'][3], places=3)
        self.assertAlmostEqual(59.365404, result['cols']['100262'][4], places=3)
        self.assertAlmostEqual(1.023541, result['cols']['100262'][5], places=3)
        self.assertAlmostEqual(1865.916070, result['cols']['100262'][6], places=3)

        self.assertAlmostEqual(78.12500, result['cols']['100267'][0], places=3)
        self.assertAlmostEqual(3764.20455, result['cols']['100267'][1], places=3)
        self.assertAlmostEqual(1742.89773, result['cols']['100267'][2], places=3)
        self.assertAlmostEqual(52.55682, result['cols']['100267'][3], places=3)
        self.assertAlmostEqual(433.23864, result['cols']['100267'][4], places=3)
        self.assertAlmostEqual(80.96591, result['cols']['100267'][5], places=3)
        self.assertAlmostEqual(805.39773, result['cols']['100267'][6], places=3)

        self.assertEqual(errorStr, None)
        self.assertEqual(response_status, 200)

if __name__ == '__main__':
    unittest.main()