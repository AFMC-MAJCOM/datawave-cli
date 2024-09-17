import argparse
import click
import json
import logging
import os
import requests
import sys
from types import SimpleNamespace
from datetime import datetime
from datawave_cli.utilities import pods
from datawave_cli.utilities.cli_stuff import File, common_options
from datawave_cli.utilities.utilities import setup_logger, log_http_response


base_url = f"dwv-dictionary.{os.environ.get('DWV_URL', '')}"


def get_entire_dictionary(cert: str, auths: str, data_types: str, namespace: str, file: str,
                          log: logging.Logger = logging.getLogger('dictionary_interactions'),
                          use_ip: bool = False):
    """
    Display or save the entire dictionary of datawave, all data types.

    Parameters
    ----------
    cert: str
        the cert to use in the HTTP Request.
    namespace: str
        the kubernetes namespace to get information from.
    file: str
        the name of where to save output
    log: logging.Logger
        the log object.
    use_ip: bool
        Enables using the IP and Port instead of DNS
    """
    log.info("Getting the entire field dictionary in DataWave...")
    if use_ip:
        pod_ip = pods.get_specific_pod(pods.web_dictionary_info, namespace).pod_ip
        request = f"https://{pod_ip}:8443/dictionary/data/v1/"
    else:
        request = f"https://{base_url}/dictionary/data/v1/"
    data = {'auths': auths,
            'dataTypeFilters': data_types}
    resp = requests.get(request, data=data, cert=cert, verify=False)
    log_http_response(resp, log)
    fields = parse_response(resp)
    if file is None:
        display_dictionary(fields, log)
    else:
        save_dictionary(file, fields, log)


def parse_response(resp: requests.Response) -> dict:
    """
    Rips apart the HTTP Response content and pulls of the information we desire.

    Parameters
    ----------
    resp: requests.Response
        the HTTP Response object.

    Returns
    -------
    A dictionary containing all the information parsed out of the HTTP JSON response.
    """
    fields = []
    for field in resp.json()['MetadataFields']:
        field_name = field['fieldName']
        field_data_type = field['dataType']
        field_forward_indexed = field['forwardIndexed']
        field_reverse_indexed = field['reverseIndexed']
        field_types = field['Types']
        field_description = field['Descriptions']
        field_index_only = field['indexOnly']
        field_normalized = field['normalized']
        field_tokenized = field['tokenized']
        field_last_updated = field['lastUpdated']

        fields.append({"name": field_name, "Data Type": field_data_type, "Forward Indexed": field_forward_indexed,
                       "Reversed Indexed": field_reverse_indexed, "Types": field_types,
                       "Tokenized": field_tokenized, "Normalized": field_normalized, "Index Only": field_index_only,
                       "Descriptions": field_description, "Last Updated": field_last_updated})
    return fields


def display_dictionary(fields: list,
                       log: logging.Logger = logging.getLogger('dictionary_interactions')):
    """
    Displays a dictionary of all the fields found from the query.

    Parameters
    ----------
    fields: list
        the fields returned from the filter function.
    log: logging.Logging
        The logger object.
    """
    if not fields:
        log.warning("No fields to display.")
        return
    dictionary_keys = fields[0].keys()
    max_lengths = {}
    # Build Header
    header = ""
    row_split = ""
    for key in dictionary_keys:
        max_lengths[key] = max(max([len(str(f[key])) for f in fields]), len(key))
        header += f"{key:{max_lengths[key]}}|"
        row_split += "-" * max_lengths[key] + "|"
    log.info(header)
    log.info(row_split)
    for field in fields:
        row = ""
        for key in dictionary_keys:
            row += f"{str(field[key]):{max_lengths[key]}}|"
        log.info(row)


def save_dictionary(filename: str, fields: list,
                    log: logging.Logger = logging.getLogger('dictionary_interactions')):
    """
    Saves a dictionary of all the fields found from the query.

    Parameters
    ----------
    filename: str
        the name where to save the dictionary output
    fields: list
        the fields returned from the filter function.
    log: logging.Logger
        the log object.
    """
    if not fields:
        log.warning("No fields to display.")
        return
    dictionary_keys = fields[0].keys()
    max_lengths = {}
    header = ""
    row_split = ""
    for key in dictionary_keys:
        max_lengths[key] = max(max([len(str(f[key])) for f in fields]), len(key))
        header += f"{key:{max_lengths[key]}}|"
        row_split += "-" * max_lengths[key] + "|"
    with open(filename, 'a') as file:
        print(header, file=file)
        print(row_split, file=file)
        for field in fields:
            row = ""
            for key in dictionary_keys:
                row += f"{str(field[key]):{max_lengths[key]}}|"
            print(row, file=file)


def main(args):
    log = setup_logger("dictionary_interactions", log_level=args.log_level)
    global base_url
    base_url = f"dwv-dictionary.{args.url}"

    if args.key is None:
        cert = args.cert
    else:
        cert = (args.cert, args.key)

    get_entire_dictionary(cert, args.auths, args.data_types, args.namespace, args.output, log=log, use_ip=args.ip)


@click.command
@common_options
@click.option("--auths", type=str, required=True,
              help="The auths used when retrieving results. Should be a comma-delineated list without spaces.")
@click.option("-d", "--data-types", type=str,
              help="The datatypes to filter for. This CAN be a comma delineated list with no spaces.")
@click.option("-o", "--output", type=File(file_type='.txt'),
              help="The location to output the results of the dictionary. Not setting this "
              + "means console output only.")
def dictionary(**kwargs):
    """Displays the dictionary of fields in Datawave.

    If a specific datatype is provided, it will display only the fields for that datatype.
    """
    main(SimpleNamespace(**kwargs))


if __name__ == "__main__":
    dictionary(sys.argv[1:])