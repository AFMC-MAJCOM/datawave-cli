import json
import logging
import sys
from types import SimpleNamespace

import click
import requests

from datawave_cli.base_interactions import BaseInteractions
from datawave_cli.utilities import pods
from datawave_cli.utilities.cli_stuff import common_options
from datawave_cli.utilities.utilities import setup_logger, log_http_response


class AuthorizationInteractions(BaseInteractions):
    def __init__(self, args, log: logging.Logger = logging.getLogger('authorizations_interactions')):
        self.log = log
        super().__init__(args)

    def get_pod_ip(self):
        return pods.get_specific_pod(pods.web_authorization_info, self.namespace).pod_ip

    def authorization_whoami(self):
        """
        Calls the /whoami endpoint for DataWave authorizations and displays the
        information prettily.
        """
        self.log.info("Getting the authorization details for my cert from DW...")
        request = f"{self.base_url}/authorization/v1/whoami"
        resp = requests.get(request, cert=self.cert, headers=self.headers, verify=False)
        log_http_response(resp, self.log)
        try:
            self.log.info(json.dumps(resp.json(), indent=1))
            return resp.json()
        except requests.decoder.JSONDecodeError:
            self.log.info(resp.text)


def main(args):
    log = setup_logger("authorizations_interactions", log_level=args.log_level)
    ai = AuthorizationInteractions(args, log)

    return ai.authorization_whoami()


@click.command
@common_options
def authorization(**kwargs):
    """Prints the results of the whoami endpoint for the provided user."""
    return main(SimpleNamespace(**kwargs))


if __name__ == "__main__":
    authorization(sys.argv[1:])