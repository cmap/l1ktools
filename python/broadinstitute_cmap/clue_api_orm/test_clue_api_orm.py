import unittest
import setup_logger
import logging
import clue_api_orm


logger = logging.getLogger(setup_logger.LOGGER_NAME)

#use the DEV api for testing and the demo user_key
cao = clue_api_orm.ClueApiOrm(base_url="https://dev-api.clue.io/api", user_key="582724ad750c4649b2edf420dc68f635")


class TestClueApiOrm(unittest.TestCase):
    def test_run_query(self):
        #get one gene
        r = cao.run_query("genes", {"where":{"pr_gene_id":5720}})
        self.assertIsNotNone(r)
        logger.debug("len(r):  {}".format(len(r)))
        logger.debug("r:  {}".format(r))
        self.assertEqual(1, len(r))

        #get multiple genes
        r = cao.run_query("genes", {"where":{"pr_gene_id":{"inq":[5720,207]}}})
        self.assertIsNotNone(r)
        logger.debug("len(r):  {}".format(len(r)))
        logger.debug("r:  {}".format(r))
        self.assertEqual(2, len(r))

    def test_run_query_handle_fail(self):
        with self.assertRaises(Exception) as context:
            cao.run_query("fakeresource", filter={})
        self.assertIsNotNone(context.exception)
        logger.debug("context.exception:  {}".format(context.exception))
        self.assertIn("ClueApiOrm run_query request failed", str(context.exception))


if __name__ == "__main__":
    setup_logger.setup(verbose=True)

    unittest.main()
