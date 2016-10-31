import unittest
import setup_logger
import logging
import clue_api_client
import os.path
import ConfigParser
import collections

__authors__ = "David L. Lahr"
__email__ = "dlahr@broadinstitute.org"


logger = logging.getLogger(setup_logger.LOGGER_NAME)

config_filepath = os.path.expanduser("~/.l1ktools_python.cfg")
config_section = "test"
cao = None


class TestClueApiClient(unittest.TestCase):
    def test_run_query(self):
        #get one gene
        r = cao.run_filter_query("genes", {"where":{"pr_gene_id":5720}})
        self.assertIsNotNone(r)
        logger.debug("len(r):  {}".format(len(r)))
        logger.debug("r:  {}".format(r))
        self.assertEqual(1, len(r))

        #get multiple genes
        r = cao.run_filter_query("genes", {"where":{"pr_gene_id":{"inq":[5720,207]}}})
        self.assertIsNotNone(r)
        logger.debug("len(r):  {}".format(len(r)))
        logger.debug("r:  {}".format(r))
        self.assertEqual(2, len(r))

        r = cao.run_filter_query("perts", {"where":{"pert_id":"BRD-K12345678"}})
        self.assertIsNotNone(r)
        logger.debug("len(r):  {}".format(len(r)))
        self.assertEqual(0, len(r))

    def test_run_query_handle_fail(self):
        with self.assertRaises(Exception) as context:
            cao.run_filter_query("fakeresource", {})
        self.assertIsNotNone(context.exception)
        logger.debug("context.exception:  {}".format(context.exception))
        self.assertIn("ClueApiClient request failed", str(context.exception))

    def test_run_where_query(self):
        r = cao.run_count_query("cells", {"cell_id":"A375"})
        self.assertIsNotNone(r)
        logger.debug("r:  {}".format(r))
        self.assertIn("count", r)
        self.assertEqual(1, r["count"])

    def test__check_request_response(self):
        FakeResponse = collections.namedtuple("FakeResponse", ["status_code", "reason"])

        #happy path
        fr = FakeResponse(200, None)
        clue_api_client.ClueApiClient._check_request_response(fr)

        #response status code that should cause failure
        fr2 = FakeResponse(404, "I don't need a good reason!")
        with self.assertRaises(Exception) as context:
            clue_api_client.ClueApiClient._check_request_response(fr2)
        logger.debug("context.exception:  {}".format(context.exception))
        self.assertIn(str(fr2.status_code), str(context.exception))
        self.assertIn(fr2.reason, str(context.exception))


def build_clue_api_client_from_default_test_config():
    cfg = ConfigParser.RawConfigParser()
    cfg.read(config_filepath)
    cao = clue_api_client.ClueApiClient(base_url=cfg.get(config_section, "clue_api_url"),
                                  user_key=cfg.get(config_section, "clue_api_user_key"))
    return cao


if __name__ == "__main__":
    setup_logger.setup(verbose=True)

    cao = build_clue_api_client_from_default_test_config()

    unittest.main()
