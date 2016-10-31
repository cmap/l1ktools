import logging
import setup_logger
import clue_api_client

__authors__ = "David L. Lahr"
__email__ = "dlahr@broadinstitute.org"


logger = logging.getLogger(setup_logger.LOGGER_NAME)


class MockClueApiClient(clue_api_client.ClueApiClient):
    def __init__(self, base_url=None, user_key=None, run_query_return_values=None):
        super(MockClueApiClient, self).__init__(base_url=base_url, user_key=user_key)

        if run_query_return_values:
            self.run_query_return_values = run_query_return_values
        else:
            self.run_query_return_values = []

    def run_filter_query(self, resource_name, filter_clause):
        return self.run_query_return_values

    def run_count_query(self, resource_name, where_clause):
        return self.run_query_return_values
