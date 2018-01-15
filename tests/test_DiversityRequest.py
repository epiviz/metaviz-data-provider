"""
   Tests for Diversity Request of metaviz data provider
   Referenced the following resources during development:
   http://flask.pocoo.org/docs/0.12/testing/
   https://docs.python.org/2/library/unittest.html
   https://github.com/pallets/flask/blob/master/examples/flaskr/tests/test_flaskr.py
"""

import unittest
from dataprovider.DiversityRequest import DiversityRequest
import dataprovider.__init__

class DiversityRequestTestCase(unittest.TestCase):

    def setUp(self):
        self.param_method = "diversity"
        self.request_values = {"params[datasource]": "msd16s",
                               "params[selectedLevels]": {"2":2},
                               "params[measurements]": u'["100259","100262","100267"]'
                              }

    def test_measurements_request(self):
        metaviz_request = dataprovider.create_request(self.param_method, self.request_values)
        result, errorStr, response_status = metaviz_request.get_data()
        #Results derived from metagenomeSeq aggeratedByTaxonomy then vegan diversity
        for result_entry in result['data']:
            if result_entry['sample_id'] ==  "100259":
                self.assertAlmostEqual(1.211377, result_entry['alphaDiversity'], places=3)
            elif result_entry['sample_id'] ==  "100262":
                self.assertAlmostEqual(1.184751, result_entry['alphaDiversity'], places=3)
            elif result_entry['sample_id'] == "100267":
                self.assertAlmostEqual(1.240743, result_entry['alphaDiversity'], places=3)

        self.assertEqual(errorStr, None)
        self.assertEqual(response_status, 200)

if __name__ == '__main__':
    unittest.main()

