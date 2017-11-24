import mpdsq
import unittest


class QueryTest(unittest.TestCase):
    def test_structures(self):
        qe = mpdsq.MPDSQueryEngine()
        query = {
            "elements": "Ti-O",
            "classes": "binary",
            "props": "atomic structure",
            "sgs": 136
        }
        results = list(qe.structures.find(query))
        self.assertGreater(len(results), 0)

    def test_properties(self):
        qe = mpdsq.MPDSQueryEngine()
        query = {
            "elements": "Ti-O",
            "classes": "binary",
            "sgs": 136
        }
        results = list(qe.properties.find(query))
        self.assertGreater(len(results), 0)
