import logging
import os
import sys
from types import SimpleNamespace

import click
import requests

from .base_interactions import BaseInteractions
from datawave_cli.utilities import pods
from datawave_cli.utilities.cli_stuff import File, common_options
from datawave_cli.utilities.utilities import setup_logger, log_http_response


base_url = f"dwv-dictionary.{os.environ.get('DWV_URL', '')}"


class DictionaryInteractions(BaseInteractions):
    def __init__(self, args, log: logging.Logger = logging.getLogger('dictionary_interactions')):
        self.log = log
        super().__init__(args)

    def get_pod_ip(self):
        return pods.get_specific_pod(pods.web_dictionary_info, self.namespace).pod_ip

    def get_entire_dictionary(self, auths: str, data_types: str, file: str):
        """
        Display or save the entire dictionary of datawave, all data types.

        Parameters
        ----------
        auths: str
            comma delimited string containing all the auths to pull the dictionary for
        data_types: str
            comma delimited string comtaining the data types to pull the dictionary for
        file: str
            the name of where to save output
        """
        self.log.info("Getting the entire field dictionary in DataWave...")
        request = f"{self.base_url}/dictionary/data/v1/"
        data = {'auths': auths,
                'dataTypeFilters': data_types}
        resp = requests.get(request, data=data, cert=self.cert, verify=False)
        log_http_response(resp, self.log)
        if resp.status_code == 200:
            fields = self.parse_response(resp)
            if file is None:
                self.display_dictionary(fields, self.log)
            else:
                self.save_dictionary(file, fields, self.log)
            return fields
        else:
            return {}

    def parse_response(self, resp: requests.Response) -> dict:
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

    def display_dictionary(self, fields: list,
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

    def save_dictionary(self, filename: str, fields: list,
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
    di = DictionaryInteractions(args, log)
    res = di.get_entire_dictionary(args.auths, args.data_types, args.output)
    return res


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