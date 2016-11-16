import logging
import setup_logger

__authors__ = "David L. Lahr"
__email__ = "dlahr@broadinstitute.org"


logger = logging.getLogger(setup_logger.LOGGER_NAME)

resource_name = "genes"


def are_genes_in_api(my_clue_api_client, gene_symbols):
    """determine if genes are present in the API

    Args:
        my_clue_api_client:
        gene_symbols: collection of gene symbols to query the API with

    Returns: set of the found gene symbols

    """
    query_result = my_clue_api_client.run_filter_query(resource_name, {"where":{"pr_gene_symbol":{"inq":gene_symbols}},
                                                                        "fields":{"pr_gene_symbol":True}})
    logger.debug("query_result:  {}".format(query_result))

    r = set([x["pr_gene_symbol"] for x in query_result])
    return r