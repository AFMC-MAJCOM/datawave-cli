import base64
import click
import logging
import json
import os
import pandas as pd
import requests
import sys
from collections import defaultdict
from datetime import datetime
from dataclasses import dataclass
from io import BytesIO
from logging import Logger
from pathlib import Path
from types import SimpleNamespace
from typing import Optional
from datawave_cli.generate_html import htmlify
from datawave_cli.utilities import pods
from datawave_cli.utilities.cli_stuff import depends_on, File, common_options
from datawave_cli.utilities.utilities import setup_logger, log_http_response


base_url = f"datawave.{os.environ.get('DWV_URL', '')}"


@dataclass
class QueryParams:
    query_name: str
    query: str
    auths: str
    column_visibility: str = 'N/A'
    page_size: int = 5
    begin: str = '19700101'
    end: str = '20990101'

    def get(self):
        return {"queryName": self.query_name,
                "columnVisibility": self.column_visibility,
                "pagesize": self.page_size,
                "begin": self.begin,
                "end": self.end,
                "query": self.query,
                "auths": self.auths
                }


class QueryConnection:
    """A class representing a connection for executing queries.

    This class provides functionality to establish and manage a connection
    for executing queries on a remote server.

    Parameters
    ----------
    ip : str
        The IP address of the server to connect to.
    port : str
        The port number of the server to connect to.
    cert : str
        The path to the SSL certificate for secure communication.
    query_params : QueryParams
        An instance of the QueryParams class containing parameters for the query.
    log : Logger, optional
        An optional logger object for logging messages. If not provided,
        a default logger will be created.

    Attributes
    ----------
    cert : str
        The path to the SSL certificate for secure communication.
    query_params : QueryParams
        An instance of the QueryParams class containing parameters for the query.
    results_count : int
        The count of results returned.
    log : Logger
        The logger object used for logging messages.

    """
    create_endpoint: str = 'DataWave/Query/EventQuery/create.json'
    quuid: Optional[str] = None

    def __init__(self, cert: str, query_params: QueryParams, log: Logger = None):
        self.cert = cert
        self.query_params = query_params
        self.results_count = 0

        if log is None:
            now = datetime.now().strftime('%Y%m%d_%H%M%S')
            self.log = setup_logger(__name__, log_file=f'logs/local/{__name__}_{now}.log', log_level=logging.DEBUG)
        else:
            self.log = log

    @classmethod
    def from_ip(cls, ip: str, port: str, cert: str, query_params: QueryParams, log: Logger = None):
        global base_url
        base_url = f'{ip}:{port}'
        return cls(cert, query_params, log)

    @property
    def next_endpoint(self):
        if self.quuid is None:
            raise ValueError("Query UUID not set, cannot create the next endpoint.")
        return f'DataWave/Query/{self.quuid}/next.json'

    @property
    def close_endpoint(self):
        if self.quuid is None:
            raise ValueError("Query UUID not set, cannot create the close endpoint.")
        return f'DataWave/Query/{self.quuid}/close.json'

    def __enter__(self):
        """Enter method for context manager

        This method is called when entering a context managed by the 'with' statement.
        It initiates the creation of a query endpoint and sets the necessary attributes
        to maintain the query state.

        Returns
        -------
        self:
            The current instance with the query endpoint created.

        Raises
        ------
        RuntimeError:
            If the Datawave `create` endpoint fails with a non-200 response.
        """
        self.log.debug("Inside enter...")
        request = f'https://{base_url}/{self.create_endpoint}'
        self.log.debug(request)
        self.log.info(f'Executing with {self.query_params}')

        resp = requests.post(request, data=self.query_params.get(), cert=self.cert, verify=False)
        log_http_response(resp, self.log)
        if (resp.status_code == 200):
            self.quuid = resp.json()['Result']
            self.open = True
        else:
            self.log.error(f"Request failed - (Status Code:{resp.status_code}, Reason:{resp.reason})")
            self.log.error(f'Response Content: {resp.content}')
            raise RuntimeError(f"Create endpoint came back with non-200 response. {resp.status_code}")
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Exit method for context manager

        This method is called when exiting a context managed by the 'with' statement.
        It finalizes the query by closing the connection to the endpoint.

        Parameters
        ----------
        exc_type:
            The type of the exception raised, if any.

        exc_value: Exception
            The exception raised, if any.

        traceback:
            The traceback object representing the call stack.

        Notes
        -----
        Any exception raised within the 'with' block will be passed to this method.
        If an exception is not handled within the method, it will propagate.
        """
        self.log.debug("Inside exit...")
        request = f'https://{base_url}/{self.close_endpoint}'
        self.log.debug(request)
        requests.get(request, cert=self.cert, verify=False)
        if self.results_count:
            self.log.info(f'Total results retrieved: {self.results_count}')
        else:
            self.log.info(f'No results found!')
        self.open = False

    def __iter__(self):
        """Iterator method for iterator.

        This method is called when the object is used in an iteration context
        (e.g., in a 'for' loop).
        It ensures that the query has been started and returns the instance for
        iteration.

        Returns
        -------
        self:
            The current instance ready for iteration.

        Raises
        ------
        RuntimeError:
            If the query has not been started.
        """
        self.log.debug("Inside iter...")
        if not self.open:
            raise RuntimeError("Query has not been started!")
        return self

    def __next__(self):
        """Next method for iterator

        This method is called to retrieve the next item in the iteration sequence.
        It performs a request to fetch the next data from the query endpoint and returns it.

        Returns
        -------
            dict: The next item in the iteration sequence.

        Raises
        ------
        StopIteration:
            If there are no more items to iterate over.
            Note: this is the normal method to stop an iterator.
        """
        self.log.debug("Inside next...")
        request = f'https://{base_url}/{self.next_endpoint}'
        self.log.debug(request)
        next_resp = requests.get(request, cert=self.cert, verify=False)
        log_http_response(next_resp, self.log)
        if (next_resp.status_code == 200):
            res = next_resp.json()
            self.results_count += res['ReturnedEvents']
            return next_resp.json()
        else:
            raise StopIteration


def parse_and_filter_results(raw_events: list, *, filter_on: str):
    """Parses datawaves returned events to json format and then filters it if a key is provided.

    Parameters
    ----------
    raw_events: list[dict]
        The JSON object returned from a datawave query to be parsed out

    filter_on: str
        The key or a comma separated list of keys to filter results on.

    Returns
    -------
    A list of filtered events corresponding to the provided key.

    Raises
    ------
    KeyError:
        If the key does not exist in the dataset being filtered.
    """
    parsed = parse_results(raw_events)
    filtered = filter_results(parsed, filter_on=filter_on)
    return filtered


def parse_results(raw_events: dict):
    """Parses datawaves insane return to a more sesnsible json format.

    Datawave's return type is kind of insane. It's a dict, one of those keys is `Events` which is a list of events.
    Each element of that list of events is a dict. Those dicts have a key for `fields` which returns a list of fields.
    Each field is a dict with a key for `name` and one for `Value`. `Value` is another dict that contains a `value`.
    That final value is what we actually care about. There's a lot more information in that json that we dont care about
    so this function pairs that down to just a list of dicts.

    The returned object will simply be a list of events where each event is has the field name as a key and the field
    value as the value.

    Parameters
    ----------
    raw_events: dict
        The JSON object returned from a datawave query to be parsed out

    Returns
    -------
    parsed_events: list[dict]
        A list of events where each event is a dict of fields that have been ripped out from the input

    Note
    ----
    This does not do anything with the raw parquet binary. It just pulls out that binary. It is up to the caller of
    this function to decode that.
    """
    parsed_events = []
    for event in raw_events['Events']:
        event_data = defaultdict(list)
        for field in event['Fields']:
            field_name = field['name']
            field_value = field['Value']['value']
            event_data[field_name].append(field_value)

        # Convert single-item lists to just the item
        event_data = {key: (values[0] if len(values) == 1 else values) for key, values in event_data.items()}
        parsed_events.append(event_data)

    return parsed_events


def filter_results(results_in: list, filter_on: str):
    """Filters and returns a set of values based on the provided key.

    Args
    ----
    results_in: list[dict]
        The list of events to be filtered down.

    filter_on: str
        The key or a comma separated list of keys to filter results on.

    Returns
    -------
    A list of filtered events corresponding to the provided key.

    Raises
    ------
    KeyError:
        If the key does not exist in the dataset being filtered.
    """
    if filter_on is None:
        return results_in
    keys = filter_on.split(',')

    all_keys = {key for event in results_in for key in event.keys()}
    not_found = [key for key in keys if key not in all_keys]
    if not_found:
        print(repr(KeyError(f'{not_found} not found in any results!')))
        sys.exit(1)

    return [{key: event.get(key, "Not Found") for key in keys} for event in results_in]


def print_query_fields(query_params: QueryParams, cert: str, decode_raw: bool, filter_on: str,
                       log: Logger = logging.getLogger('query_interactions'),
                       namespace: str = pods.namespace, use_ip: bool = False):
    """
    Helper method for displaying fields of a query.

    Parameters
    ----------
    query_params: QueryParams
        the data object storing all the necessary pieces for a DataWave query.
    cert: str
        the location of the certificate to use in the HTTP Request
    decode_raw: bool
        Boolean indicating if we should decode the raw data
    log: Logger, optional
        The logger object.
    namespace: str, optional
        the namespace to perform a query against.
    use_ip: bool
        Enables using the IP and Port instead of DNS
    """
    connection = (QueryConnection.from_ip(pods.get_specific_pod(pods.web_datawave_info, namespace).pod_ip, '8443',
                                          cert, query_params, log) if use_ip
                  else QueryConnection(cert, query_params, log))
    with connection as qc:
        print_qc(qc, decode_raw, filter_on)


def print_qc(qc: QueryConnection, decode_raw: bool, filter_on: str):
    """
    Loops over the data from the query connection object and prints it to console.

    Parameters
    ----------
    qc: QueryConnection
        the object performing and storing the datawave query information
    decode_raw: bool
        Boolean indicating if we should decode the raw data
    filter_on: list | str
        List of keys or single key to filter the data on
    """
    for data in qc:
        for event in parse_and_filter_results(data, filter_on=filter_on):
            for name, value in event.items():
                if 'RAWDATA' in name:
                    if decode_raw:
                        buffer = BytesIO(base64.b64decode(value))
                        value = pd.read_parquet(buffer)
                    else:
                        value = 'Contains raw data'
                print(f'{name}: {value}')
            print('-' * 10)
    print(f'Query returned: {qc.results_count} events.')


def save_query_fields(filename: str, query_params: QueryParams, cert: str, decode_raw: bool,
                      log: Logger = logging.getLogger('query_interactions'),
                      namespace: str = pods.namespace, use_ip: bool = False):
    """
    Helper method for saving fields of a query to a file.

    Parameters
    ----------
    filename: str
        path and name to where to save the query output.
    query_params: QueryParams
        the data object storing all the necessary pieces for a DataWave query.
    cert: str
        the location of the certificate to use in the HTTP Request
    decode_raw: bool
        Boolean indicating if we should decode the raw data
    log: Logger, optional
        The logger object.
    namespace: str, optional
        the namespace to perform a query against.
    use_ip: bool
        Enables using the IP and Port instead of DNS
    """
    filepath = Path(filename)
    print(f'Outputting to {filepath.resolve()}')
    filepath.parent.mkdir(parents=True, exist_ok=True)
    if filepath.exists():
        print(f'Existing output file exists. Attempting to rename it.')
        try:
            renamed_path = filepath.with_stem(filepath.stem + '_old')
            filepath.rename(renamed_path)
        except PermissionError as e:
            # Some times it gets a permission error and fails to rename it, even if the file is not in use.
            log.critical('Failed to rename old file! Check that it is not in use or otherwise locked!')
            raise e
        print(f'Existing file renamed to {renamed_path}')

    connection = (QueryConnection.from_ip(pods.get_specific_pod(pods.web_datawave_info, namespace).pod_ip, '8443',
                                          cert, query_params, log=log) if use_ip
                  else QueryConnection(cert, query_params, log=log))
    with connection as qc:
        save_qc(qc, filename, decode_raw)

    log.info(f"Saved output file to {filepath.resolve()}")
    if decode_raw:
        log.info(f'Saved raw data to {filepath.parent}/rawdata')


def save_qc(qc: QueryConnection, filename: str, decode_raw: bool):
    """
    Loops through the data returned from the QueryConnection object and saves it to a file.

    Parameters
    ----------
    qc: QueryConnection
        the connection object to loop over
    filename: str
        The filename to write the results to
    decode_raw: bool
        Whether to decode the raw value found
    """
    events = []
    for data in qc:
        events.extend(parse_results(data))

    # Generate some query level metadata from the results
    metadata = {}
    metadata['Query'] = qc.query_params.query
    metadata['Returned Events'] = qc.results_count
    metadata['Auths'] = qc.query_params.auths
    cert = qc.cert[0] if isinstance(qc.cert, tuple) else qc.cert
    metadata['Cert'] = Path(cert).stem
    # current ms since epoch
    metadata['Unix Timestamp(ms)'] = int(datetime.now().timestamp() * 1e3)

    with open(filename, 'w') as file:
        json.dump({'metadata': metadata, 'events': events}, file, indent=2)

    if decode_raw:
        for event in events:
            for key, value in event.items():
                if 'RAWDATA' in key:
                    raw_bytes = base64.b64decode(value)
                    orig_file = event['ORIG_FILE']
                    if isinstance(orig_file, list):
                        orig_file = orig_file[0]
                    parq_dir = orig_file.split('.json', 1)[0]
                    parq_name = key.split('_', 1)[1]
                    pq_file = Path(filename).parent.joinpath('rawdata', parq_dir, f'{parq_name}.parquet')
                    pq_file.parent.mkdir(parents=True, exist_ok=True)
                    with open(pq_file, 'wb') as pq_fh:
                        pq_fh.write(raw_bytes)


def main(args):
    query_params = QueryParams(query_name=args.query_name,
                               query=args.query,
                               auths=args.auths)
    log = setup_logger('query_interactions', log_level=args.log_level)

    global base_url
    base_url = f"datawave.{args.url}"

    if args.key is None:
        cert = args.cert
    else:
        cert = (args.cert, args.key)

    if args.output is None:
        print_query_fields(query_params, cert, args.decode_raw, args.filter, namespace=args.namespace, use_ip=args.ip)
    else:
        save_query_fields(args.output, query_params, cert, args.decode_raw, namespace=args.namespace, use_ip=args.ip)

    if args.html:
        htmlify(args.output, args.query, args.auths, Path(args.output).with_suffix('.html'))


@click.command
@common_options
@click.option("-q", "--query", type=str, required=True,
              help="The actual query to perform, must conform to JEXL formatting. "
                   + "https://commons.apache.org/proper/commons-jexl/reference/syntax.html")
@click.option("--query-name", type=str, default="test-query", show_default=True,
              help="The name given to the query in the query request.")
@click.option("--auths", type=str, required=True,
              help="A comma-separated list of authorizations to use within the query request.")
@click.option("-f", "--filter", type=str, default=None, show_default=True,
              help="A single key or comma delineated list of keys without spaces to filter the data on.")
@click.option("-o", "--output", type=File(file_type='.json'), show_default=True,
              help="The .json file to output the results of the query. Not setting this means console output only.")
@click.option("--html", is_flag=True, cls=depends_on('output'),
              help="If present, indicates that a formatted HTML file should be output. This argument requres "
              + "the `-o` argument is passed as the html file is generated using it and is given the "
              + "same name and location.")
@click.option("-d", "--decode-raw", is_flag=True,
              help="If present, indicates that raw binaries should be decoded.")
def query(**kwargs):
    """Executes a query to Datawave and outputs the results.

        \b
        Default Behavior: Prints the results to the console.
        Output to File: If an output file is specified, results and query metadata are saved in a
        JSON file. Optionally, an HTML file can be generated for a formatted view of the results.

        \b
        Raw Data Handling:
          By default, raw data fields are not decoded:
            If printing to the console: Displays 'Contains Raw Data'.
            If saving to a file: The raw binary is saved to the file.
          Use the `-d` flag to decode raw data:
            If printing to the console: Displays a pandas table.
            If saving to a file: Generates Parquet files.
            Note: The raw binary content of the Parquet files is always included in the JSON output,
            regardless of the `-d` flag, allowing users to decode the data later if desired.
    """
    main(SimpleNamespace(**kwargs))


if __name__ == "__main__":
    query(sys.argv[1:])
