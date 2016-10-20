import requests
import logging
import setup_logger
import json

__authors__ = "David L. Lahr"
__email__ = "dlahr@broadinstitute.org"


logger = logging.getLogger(setup_logger.LOGGER_NAME)


class ClueApiOrm(object):
    """Basic class for running queries against CLUE api
    """

    def __init__(self, base_url=None, user_key=None):
        """
        Args:
            base_url: specific URL to use for the CLUE api, e.g. https://dev-api.clue.io/api/
            user_key: user key to use for authentication, available from CLUE account

        Returns:
        """
        self.base_url = base_url
        self.user_key = user_key

    def run_query(self, resource_name, filter):
        """run a query (get) against the CLUE api, using the API and user key fields of the object

        Args:
            resource_name: str - name of the resource / collection to query - e.g. genes, perts, cells etc.
            filter: dictionary - contains filter to pass to API to; uses loopback specification

        Returns: list of dictionaries containing the results of the query

        """
        data = {"user_key":self.user_key}
        url = self.base_url + "/" + resource_name
        params = {"filter":json.dumps(filter)}

        r = requests.get(url, data=data, params=params)
        logger.debug("requests.get result r.status_code:  {}".format(r.status_code))

        assert r.status_code == 200, "ClueApiOrm run_query request failed r.status_code:  {}  r.reason:  {}".format(
            r.status_code, r.reason)

        return r.json()