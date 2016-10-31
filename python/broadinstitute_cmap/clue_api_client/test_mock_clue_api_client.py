import unittest
import setup_logger
import logging
import mock_clue_api_client

__authors__ = "David L. Lahr"
__email__ = "dlahr@broadinstitute.org"


logger = logging.getLogger(setup_logger.LOGGER_NAME)


class TestMockClueApiClient(unittest.TestCase):
    def test_run_filter_query(self):
        mcao = mock_clue_api_client.MockClueApiClient(run_query_return_values=[{"hello":"world"}])
        r = mcao.run_filter_query("fake resource name", {"unused":"filter"})
        self.assertIsNotNone(r)
        logger.debug("r:  {}".format(r))
        self.assertEqual(1, len(r))
        r = r[0]
        self.assertEqual(1, len(r))
        self.assertIn("hello", r)
        self.assertEqual("world", r["hello"])

    def test_run_count_query(self):
        mcao = mock_clue_api_client.MockClueApiClient(run_query_return_values=[{"hello":"world"}])
        r = mcao.run_count_query("fake resource name", {"unused":"filter"})
        self.assertIsNotNone(r)
        logger.debug("r:  {}".format(r))
        self.assertEqual(1, len(r))
        r = r[0]
        self.assertEqual(1, len(r))
        self.assertIn("hello", r)
        self.assertEqual("world", r["hello"])

if __name__ == "__main__":
    setup_logger.setup(verbose=True)

    unittest.main()