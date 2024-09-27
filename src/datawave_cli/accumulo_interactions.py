import logging
import sys
from types import SimpleNamespace

import click
import requests

from .base_interactions import BaseInteractions
from datawave_cli.utilities import pods
from datawave_cli.utilities.cli_stuff import common_options
from datawave_cli.utilities.utilities import setup_logger, log_http_response


class AccumuloInteractions(BaseInteractions):
    def __init__(self, args, log: logging.Logger = logging.getLogger('accumulo_interactions')):
        self.log = log
        super().__init__(args)

    def get_pod_ip(self):
        return pods.get_specific_pod(pods.web_datawave_info, self.namespace).pod_ip

    def reload_accumulo_cache(self):
        """
        Perform the accumulo cache reload within DataWave.
        """
        self.log.info("Reloading the accumulo cache...")
        request = f"{self.base_url}/DataWave/Common/AccumuloTableCache/reload/datawave.metadata"
        resp = requests.get(request, cert=self.cert, headers=self.headers, verify=False)
        log_http_response(resp, self.log)
        self.log.info("Successfully requested a reload.")

    def view_accumulo_cache(self):
        """
        View what is happening with the accumulo cache in DataWave.

        RETURNS
        -------
        Response text from the HTTP request
        """
        self.log.info("Viewing the accumulo cache...")
        request = f"{self.base_url}/DataWave/Common/AccumuloTableCache/"
        resp = requests.get(request, cert=self.cert, headers=self.headers, verify=False)
        log_http_response(resp, self.log)
        return resp.text


def main(args):
    log = setup_logger("accumulo_interactions", log_level=args.log_level)
    ai = AccumuloInteractions(args, log=log)
    if args.view:
        text = ai.view_accumulo_cache()
        log.info(text)
    else:
        ai.reload_accumulo_cache()


@click.command
@common_options
@click.option("-v", "--view", is_flag=True, help="Flag to view the accumulo cache instead of refreshing.")
def accumulo(**kwargs):
    """Used to interface with the Accumulo cache.

    \b
    Default Behavior: Requests a refresh of the cache.
    If the `-v` flag is passed, it will instead print the status of the cache.
    """
    main(SimpleNamespace(**kwargs))


if __name__ == "__main__":
    accumulo(sys.argv[1:])