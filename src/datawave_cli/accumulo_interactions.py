import argparse
import click
import logging
import os
import requests
import sys
from types import SimpleNamespace
from datetime import datetime
from datawave_cli.utilities import pods
from datawave_cli.utilities.cli_stuff import common_options
from datawave_cli.utilities.utilities import setup_logger, log_http_response


base_url = f"datawave.{os.environ.get('DWV_URL', '')}"


def reload_accumulo_cache(cert: str, namespace: str,
                          log: logging.Logger = logging.getLogger('accumulo_interactions'),
                          use_ip: bool = False):
    """
    Perform the accumulo cache reload within DataWave.

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
    log.info("Reloading the accumulo cache...")
    if use_ip:
        pod_ip = pods.get_specific_pod(pods.web_datawave_info, namespace).pod_ip
        request = f"https://{pod_ip}:8443/DataWave/Common/AccumuloTableCache/reload/datawave.metadata"
    else:
        request = f"https://{base_url}/DataWave/Common/AccumuloTableCache/reload/datawave.metadata"
    resp = requests.get(request, cert=cert, verify=False)
    log_http_response(resp, log)
    log.info("Successfully requested a reload.")


def view_accumulo_cache(cert: str, namespace: str,
                        log: logging.Logger = logging.getLogger('accumulo_interactions'),
                        use_ip: bool = False):
    """
    View what is happening with the accumulo cache in DataWave.

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

    RETURNS
    -------
    Response text from the HTTP request
    """
    log.info("Viewing the accumulo cache...")
    if use_ip:
        pod_ip = pods.get_specific_pod(pods.web_datawave_info, namespace).pod_ip
        request = f"https://{pod_ip}:8443/DataWave/Common/AccumuloTableCache/"
    else:
        request = f"https://{base_url}/DataWave/Common/AccumuloTableCache/"
    resp = requests.get(request, cert=cert, verify=False)
    log_http_response(resp, log)
    return resp.text


def main(args):
    log = setup_logger("accumulo_interactions", log_level=args.log_level)
    global base_url
    base_url = f"datawave.{args.url}"

    if args.key is None:
        cert = args.cert
    else:
        cert = (args.cert, args.key)

    if args.view:
        text = view_accumulo_cache(cert, args.namespace, log=log, use_ip=args.ip)
        log.info(text)
    else:
        reload_accumulo_cache(cert, args.namespace, log=log, use_ip=args.ip)


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