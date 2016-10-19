import logging
import setup_logger
import clue_api_orm

__authors__ = "David L. Lahr"
__email__ = "dlahr@broadinstitute.org"


logger = logging.getLogger(setup_logger.LOGGER_NAME)


class MockClueApiOrm(clue_api_orm.ClueApiOrm):
    def __init__(self, base_url=None, user_key=None, run_query_return_values=None):
        super(MockClueApiOrm, self).__init__(base_url=base_url, user_key=user_key)

        if run_query_return_values:
            self.run_query_return_values = run_query_return_values
        else:
            self.run_query_return_values = []

    def run_query(self, resource_name, filter):
        return self.run_query_return_values