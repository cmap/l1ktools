import requests
import logging
import setup_logger
import json

__authors__ = "David L. Lahr"
__email__ = "dlahr@broadinstitute.org"


logger = logging.getLogger(setup_logger.LOGGER_NAME)


class ClueApiClient(object):
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

    def run_filter_query(self, resource_name, filter_clause):
        """run a query (get) against the CLUE api, using the API and user key fields of self and the fitler_clause provided

        Args:
            resource_name: str - name of the resource / collection to query - e.g. genes, perts, cells etc.
            filter_clause: dictionary - contains filter to pass to API to; uses loopback specification

        Returns: list of dictionaries containing the results of the query
        """
        data = {"user_key":self.user_key}
        url = self.base_url + "/" + resource_name
        params = {"filter":json.dumps(filter_clause)}

        r = requests.get(url, data=data, params=params)
        logger.debug("requests.get result r.status_code:  {}".format(r.status_code))

        assert r.status_code == 200, "ClueApiOrm run_query request failed r.status_code:  {}  r.reason:  {}".format(
            r.status_code, r.reason)

        return r.json()

    def run_count_query(self, resource_name, where_clause):
        """run a query (get) against CLUE api

        Args:
            resource_name: str - name of the resource / collection to query - e.g. genes, perts, cells etc.
            where_clause: dictionary - contains where clause to pass to API to; uses loopback specification

        Returns: dictionary containing the results of the query
        """
        data = {"user_key":self.user_key}
        url = self.base_url + "/" + resource_name + "/count"
        params = {"where":json.dumps(where_clause)}

        r = requests.get(url, data=data, params=params)
        logger.debug("requests.get result r.status_code:  {}".format(r.status_code))

        assert r.status_code == 200, "ClueApiOrm run_query request failed r.status_code:  {}  r.reason:  {}".format(
            r.status_code, r.reason)

        return r.json()
