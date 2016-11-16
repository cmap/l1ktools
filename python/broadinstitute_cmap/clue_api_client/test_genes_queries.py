import unittest
import setup_logger
import logging
import test_clue_api_client
import gene_queries as gq

__authors__ = "David L. Lahr"
__email__ = "dlahr@broadinstitute.org"


logger = logging.getLogger(setup_logger.LOGGER_NAME)

cao = None


class TestGeneQueries(unittest.TestCase):
    def test_are_genes_in_api(self):
        r = gq.are_genes_in_api(cao, ["AKT1", "BRAF", "Dave Lahr's fake cell line that never existed"])
        logger.debug("r:  {}".format(r))
        self.assertIsNotNone(r)
        self.assertEqual(2, len(r))
        self.assertIn("AKT1", r)
        self.assertIn("BRAF", r)



if __name__ == "__main__":
    setup_logger.setup(verbose=True)

    cao = test_clue_api_client.build_clue_api_client_from_default_test_config()

    unittest.main()