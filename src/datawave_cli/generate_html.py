import json
import os
import webbrowser
from collections import defaultdict
from pathlib import Path


keys_to_ignore = []
# Columns to put at the start of the table
front_columns = ['NAME', 'VISIBILITY']
# Columns to put at the end of the table. Currently these are the datawave internal columns
end_columns = ['TERM_COUNT', 'LOAD_DATE', 'ORIG_FILE', 'RECORD_ID']


def gen_html(headers: set, events: list, metadata: dict,
             template_path: os.PathLike | str = 'resources/template.html'):
    """Creates a string of an html file from a set of parameters

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

    template_path: os.PathLike | str
        The path to the html file to use as a template. This can be any HTML file as long as it has the correct
        placeholder comments to be replaced. Defaults to resources/template.html.

    Returns
    -------
    html_output: str
        The string of an HTML file.
    """
    with open(template_path, 'r') as file:
        html_template = file.read()

    # Generate query stuff from the metadata
    metadata_html = '<table>'
    metadata_html += '<tr>'
    for key in metadata.keys():
        metadata_html += f'<th>{key}</th>'
    metadata_html += '</tr>'
    metadata_html += '<tr>'
    for value in metadata.values():
        metadata_html += f'<td>{value}</td>'
    metadata_html += '</tr>'
    metadata_html += '</table>'
    html_output = html_template.replace('<!-- METADATA PLACEHOLDER -->', metadata_html)

    # Organize the headers based on lists defined at top of file
    ordered_headers = [header for header in front_columns if header in headers]
    ordered_headers += [header for header in headers if header not in (front_columns + end_columns)]
    ordered_headers += [header for header in end_columns if header in headers]

    # Generate table header stuff
    header_html = '<tr>\n'
    for header in ordered_headers:
        header_html += f'<th>{header}</th>\n'
    header_html += '</tr>\n'

    html_output = html_output.replace('<!-- HEADER PLACEHOLDER -->', header_html)

    # Generate table body stuff
    table_html = ''
    for event in events:
        table_html += '<tr>\n'
        for header in ordered_headers:
            value = event.get(header, 'N/A')
            if isinstance(value, list):
                value = ', '.join(value)
            table_html += f'<td>{value}</td>\n'
        table_html += '</tr>\n'

    html_output = html_output.replace('<!-- TABLE PLACEHOLDER -->', table_html)

    return html_output


def htmlify(input_file: os.PathLike | str, out_file: str = 'resources/output.html'):
    """Wrapper for the htmlification process.

    This will take the output file of a datawave_cli query and output an html file with a formatted table of events.

    Parameters
    ----------
    input_file: os.PathLike | str
        A path to the file output of a datawave query.

    out_file: str, Optional
        The path of the output file. Defaults to `./resources/output.html`
    """
    with open(input_file, 'r') as f:
        raw_data = json.load(f)

    headers = {key for event in raw_data['events'] for key in event.keys()}

    html_output = gen_html(headers, raw_data['events'], metadata=raw_data['metadata'])

    with open(out_file, 'w') as f:
        f.write(html_output)

    # Note: this line does not work in WSL but is being left cause it does work for powershell
    webbrowser.open(f'file://{Path(out_file).resolve()}')


if __name__ == '__main__':
    htmlify(r'resources/sample.json')
