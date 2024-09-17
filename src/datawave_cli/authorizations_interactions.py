import click
import json
import logging
import os
import requests
import sys
from datetime import datetime
from types import SimpleNamespace
from datawave_cli.utilities import pods
from datawave_cli.utilities.cli_stuff import common_options
from datawave_cli.utilities.utilities import setup_logger, log_http_response


base_url = f"dwv-authorization.{os.environ.get('DWV_URL', '')}"


def authorization_whoami(cert: str, namespace: str,
                         log: logging.Logger = logging.getLogger('authorizations_interactions'),
                         use_ip: bool = False):
    """
    Calls the /whoami endpoint for DataWave authorizations and displays the
    information prettily.

    Parameters
    ----------
    cert: str
        the cert to use in the HTTP Request.
    namespace: str
        the kubernetes namespace to get information from.
    log: logging.Logger
        The logger object.
    use_ip: bool
        Enables using the IP and Port instead of DNS
    """
    log.info("Getting the authorization details for my cert from DW...")
    if use_ip:
        pod_ip = pods.get_specific_pod(pods.web_authorization_info, namespace).pod_ip
        request = f"https://{pod_ip}:8443/authorization/v1/whoami"
    else:
        request = f"https://{base_url}/authorization/v1/whoami"
    resp = requests.get(request, cert=cert, verify=False)
    log_http_response(resp, log)
    try:
        log.info(json.dumps(resp.json(), indent=1))
    except requests.decoder.JSONDecodeError:
        log.info(resp.text)


def main(args):
    log = setup_logger("authorizations_interactions", log_level=args.log_level)

    global base_url
    base_url = f"dwv-authorization.{args.url}"

    if args.key is None:
        cert = args.cert
    else:
        cert = (args.cert, args.key)

    authorization_whoami(cert, args.namespace, log, args.ip)


@click.command
@common_options
def authorization(**kwargs):
    """Prints the results of the whoami endpoint for the provided user."""
    main(SimpleNamespace(**kwargs))


if __name__ == "__main__":
    authorization(sys.argv[1:])