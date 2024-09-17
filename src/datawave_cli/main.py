import click
import sys
import urllib3
from importlib.metadata import version
from types import SimpleNamespace
from datawave_cli.utilities.cli_stuff import depends_on, File, common_options
from datawave_cli.authorizations_interactions import authorization
from datawave_cli.ingest_interactions import ingest
from datawave_cli.accumulo_interactions import accumulo
from datawave_cli.dictionary_interactions import dictionary
from datawave_cli.query_interactions import query


@click.group(context_settings=dict(help_option_names=['-h', '--help']))
@click.version_option(version('datawave_cli'), '-v', '--version')
@click.option('-s', '--suppress-warning', is_flag=True, hidden=True,
              help="If passed will suppress the certificate verification warning. Does not show on --help.")
def main(**kwargs):
    if kwargs.pop('suppress_warning'):
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


main.add_command(authorization)
main.add_command(ingest)
main.add_command(query)
main.add_command(accumulo)
main.add_command(dictionary)


if __name__ == "__main__":
    main(sys.argv[1:])