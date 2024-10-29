import logging
import sys
from functools import partial
from types import SimpleNamespace
from typing import Callable

import click
import requests
from requests.exceptions import HTTPError, JSONDecodeError, Timeout

from datawave_cli.base_interactions import BaseInteractions
from datawave_cli.utilities import pods
from datawave_cli.utilities.cli_stuff import File, common_options
from datawave_cli.utilities.utilities import setup_logger, log_http_response


class DictionaryInteractions(BaseInteractions):
    pod_info = pods.web_dictionary_info

    def __init__(self, args, log: logging.Logger = logging.getLogger('dictionary_interactions')):
        self.log = log
        super().__init__(args)

    def get_dictionary(self, auths: str, data_types: str):
        """Retrieves the dictionary from datawave for the provided data types.

        Parameters
        ----------
        auths: str
            comma delimited string containing all the auths to pull the dictionary with
        data_types: str
            comma delimited string comtaining the data types to pull the dictionary for

        Returns
        -------
        A dictionary containing the json of the response from datawave

        Raises
        ------
        RuntimeError:
            Raised if an invalid response is recieved from the request
        """
        self.log.info("Getting the entire field dictionary in DataWave...")
        request = f"{self.base_url}/dictionary/data/v1/"
        data = {'auths': auths,
                'dataTypeFilters': data_types}
        self.log.debug(f'Hitting {request} with {data} and {self.headers}')
        try:
            resp = requests.get(request, data=data, cert=self.cert, headers=self.headers, verify=False)
            log_http_response(resp, self.log)
            resp.raise_for_status()
            return resp.json()
        except (HTTPError, JSONDecodeError, Timeout) as e:
            msg = 'Invalid response from dictionary request'
            self.log.error(f'{msg}: {e}')
            raise RuntimeError(msg) from e

    def parse_response(self, resp: dict) -> dict:
        """Rips apart the HTTP Response content and pulls out the information we desire.

        Parameters
        ----------
        resp: dict
            The json of the response object.

        Returns
        -------
        A list of dictionaries containing all the information parsed out of the response.
        """
        fields = []
        for field in resp['MetadataFields']:
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

    def format_dictionary(self, fields: list):
        """Formats the dictionary ouput into header, row separator, and rows.

        Parameters
        ----------
        fields: list
            the fields returned from the filter function.

        Returns
        -------
        header: str
            The header for the table.
        row_split: str
            A splitter to divide header from body.
        rows: list[str]
            A list of strings representing the dictionary data.
        """
        if not fields:
            self.log.warning('No fields provided, returning Nones')
            return None, None, [None]

        dict_keys = fields[0].keys()
        max_lengths = {}

        header = ""
        row_split = ""

        for key in dict_keys:
            max_lengths[key] = max(max([len(str(f[key])) for f in fields]), len(key))
            header += f"{key:{max_lengths[key]}}|"
            row_split += "-" * max_lengths[key] + "|"

        rows = []
        for field in fields:
            row = ""
            for key in dict_keys:
                row += f"{str(field[key]):{max_lengths[key]}}|"
            rows.append(row)

        return header, row_split, rows

    def output_dictionary(self, writer: Callable[[str], None], fields: list):
        """Generalized function to output the dictionary using the writer_func provided.

        Parameters
        ----------
        writer_func : Callable
            Function that handles writing (log.info, print, etc.).
        fields : list
            The fields returned from the filter function.
        """
        if not fields:
            self.log.warning("No fields to display")
            return None

        header, row_split, rows = self.format_dictionary(fields)

        writer(header)
        writer(row_split)
        for row in rows:
            writer(row)


def main(args):
    """Creates a DI object and routes the results based on `args.output`"""
    log = setup_logger("dictionary_interactions", log_level=args.log_level)
    di = DictionaryInteractions(args, log)
    resp = di.get_dictionary(args.auths, args.data_types)
    fields = di.parse_response(resp)
    if args.output is None:
        di.output_dictionary(di.log.info, fields)
    else:
        with open(args.output, 'w') as file:
            writer = partial(print, file=file)
            di.output_dictionary(writer, fields)
    return fields


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
    return main(SimpleNamespace(**kwargs))


if __name__ == "__main__":
    dictionary(sys.argv[1:])