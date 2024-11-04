import json
import logging
import sys
from types import SimpleNamespace

import click
import requests
from requests.exceptions import HTTPError, JSONDecodeError, Timeout

from datawave_cli.base_interactions import BaseInteractions
from datawave_cli.utilities import pods
from datawave_cli.utilities.cli_stuff import common_options
from datawave_cli.utilities.utilities import setup_logger, log_http_response


class AuthorizationInteractions(BaseInteractions):
    def __init__(self, args, log: logging.Logger = logging.getLogger('authorizations_interactions')):
        self.log = log
        super().__init__(args)

    @property
    def pod_info(self):
        return pods.web_authorization_info

    def authorization_whoami(self):
        """
        Calls the /whoami endpoint for DataWave authorizations and displays the
        information prettily.
        """
        self.log.info("Getting the authorization details for my cert from DW...")
        request = f"{self.base_url}/authorization/v1/whoami"
        try:
            resp = requests.get(request, cert=self.cert, headers=self.headers, verify=False)
            resp.raise_for_status()
            log_http_response(resp, self.log)
            self.log.info(json.dumps(resp.json(), indent=1))
            return resp.json()
        except (HTTPError, JSONDecodeError, Timeout) as e:
            msg = "A bad response from the endpoint whoami was found"
            self.log.error(f"{msg}: {e}")
            raise RuntimeError(msg) from e

    def authorization_evict_users(self):
        """
        Calls the /admin/evictUsers within the Datawave authorization to evict
        all users within the cache.
        """
        self.log.info("Requesting all users to be evicted from DW...")
        request = f"{self.base_url}/authorization/v1/admin/evictAll"
        try:
            resp = requests.get(request, cert=self.cert, headers=self.headers, verify=False)
            resp.raise_for_status()
            log_http_response(resp, self.log)
            return resp
        except (HTTPError, JSONDecodeError, Timeout) as e:
            msg = "An error occurred while requesting to evict all users"
            self.log.error(f"{msg}: {e}")
            raise RuntimeError(msg) from e


def main(args):
    log = setup_logger("authorizations_interactions", log_level=args.log_level)
    ai = AuthorizationInteractions(args, log)

    if args.whoami:
        return ai.authorization_whoami()
    elif args.evict_users:
        return ai.authorization_evict_users()
    else:
        log.warning("No cmd given, exiting without running anything. Pass '--whoami' or '--evict_users'.")


@click.command
@common_options
@click.option("--whoami", is_flag=True, help="Flag to denote to run the whoami endpoint.")
@click.option("--evict_users", is_flag=True, help="Flag to denote to run the evictUsers endpoint.")
def authorization(**kwargs):
    """Prints the results of the whoami endpoint for the provided user."""
    return main(SimpleNamespace(**kwargs))


if __name__ == "__main__":
    authorization(sys.argv[1:])