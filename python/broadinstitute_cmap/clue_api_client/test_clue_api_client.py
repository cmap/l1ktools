import unittest
import setup_logger
import logging
import clue_api_client
import os.path
import ConfigParser

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
        self.assertIn("ClueApiOrm run_query request failed", str(context.exception))

    def test_run_where_query(self):
        r = cao.run_count_query("cells", {"cell_id":"A375"})
        self.assertIsNotNone(r)
        logger.debug("r:  {}".format(r))
        self.assertIn("count", r)
        self.assertEqual(1, r["count"])


if __name__ == "__main__":
    setup_logger.setup(verbose=True)

    cfg = ConfigParser.RawConfigParser()
    cfg.read(config_filepath)
    cao = clue_api_client.ClueApiClient(base_url=cfg.get(config_section, "clue_api_url"),
                                  user_key=cfg.get(config_section, "clue_api_user_key"))

    unittest.main()
