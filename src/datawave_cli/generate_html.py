import json
import os


keys_to_ignore = []
# Columns to put at the start of the table
front_columns = ['NAME', 'VISIBILITY']
# Columns to put at the end of the table. Currently these are the datawave internal columns
end_columns = ['TERM_COUNT', 'LOAD_DATE', 'ORIG_FILE', 'RECORD_ID']


def gen_html(headers: set, events: list, metadata: dict):
    """Creates html tables for query metadata and results from a set of headers and events.

    Not every event needs to contain every header from the headers set.
    If a key is not found for an event it will display 'N/A' as the value for that cell.

    Header order within a query is not guaranteed and as such is controlled by the lists at the start of the file.

    Parameters
    ----------
    headers: set
        Set of headers to use for the table. This is expected to contain all columns to be displayed.

    events: list
        List of dictionaries corresponding to events from the query.

    metadata: dict
        The metadata information from the datawave_cli output file.

    Returns
    -------
    html_output: dict
        A dictionary containing html tables for `metadata_table` and `results_table`.
    """
    html_output = {}

    # Generate query stuff from the metadata
    metadata_html = '<table>\n'
    metadata_html += '  <tr>\n'
    for key in metadata.keys():
        metadata_html += f'    <th>{key}</th>\n'
    metadata_html += '  </tr>\n'
    metadata_html += '  <tr>\n'
    for value in metadata.values():
        metadata_html += f'    <td>{value}</td>\n'
    metadata_html += '  </tr>\n'
    metadata_html += '</table>\n'
    html_output['metadata_table'] = metadata_html

    # Organize the headers based on lists defined at top of file
    ordered_headers = [header for header in front_columns if header in headers]
    ordered_headers += [header for header in headers if header not in (front_columns + end_columns)]
    ordered_headers += [header for header in end_columns if header in headers]

    # Generate table header stuff
    table_header = '  <tr>\n'
    for header in ordered_headers:
        table_header += f'    <th>{header}</th>\n'
    table_header += '  </tr>\n'

    # Generate table body stuff
    table_body = ''
    for event in events:
        table_body += '  <tr>\n'
        for header in ordered_headers:
            value = event.get(header, 'N/A')
            if isinstance(value, list):
                value = ', '.join(value)
            table_body += f'    <td>{value}</td>\n'
        table_body += '  </tr>\n'

    results_table = '<table>\n'
    results_table += table_header
    results_table += table_body
    results_table += '</table>\n'

    html_output['results_table'] = results_table

    return html_output


def htmlify(input_file: os.PathLike | str):
    """Wrapper for the htmlification process.

    This will take the output file of a datawave_cli query and output an html file with a formatted table of events.

    Parameters
    ----------
    input_file: os.PathLike | str
        A path to the file output of a datawave query.

    out_file: str, Optional
        The path of the output file. Defaults to `./resources/output.html`

    Returns
    -------
    A dictionary containing the two generated HTML tables.
    """
    with open(input_file, 'r') as f:
        raw_data = json.load(f)

    headers = {key for event in raw_data['events'] for key in event.keys()}

    html_output = gen_html(headers, raw_data['events'], metadata=raw_data['metadata'])

    return html_output


if __name__ == '__main__':
    res = htmlify(r'resources/sample.json')
    print(res['metadata_table'])
    print(res['results_table'])
