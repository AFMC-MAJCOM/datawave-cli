import logging
import re
import shutil
import subprocess
import sys
from io import StringIO
from pathlib import Path
from types import SimpleNamespace

import click
import pandas as pd

from datawave_cli.utilities import pods
from datawave_cli.utilities.cli_stuff import depends_on, File
from datawave_cli.utilities.utilities import setup_logger, Retry


@Retry(time_limit_min=.3)
def check_app_statuses(baseline_num_apps: int, namespace: str,
                       log: logging.Logger = logging.getLogger('ingest_interactions')):
    """
    Grabs the Hadoop Yarn applications and checks the statuses of each
    application found.

    Retry wrapper will recheck pod every 5 seconds for 3 minutes. This is a
    blocking wait.

    Parameters
    ----------
    baseline_num_apps: int
        the number of apps that were found in Hadoop Yarn before ingesting
        data.
    namespace: str
        the kubernetes namespace to perform the actions for.
    log: logging.Logger
        The logger object.

    Raises
    ------
    RuntimeError
        Raised if the any of the applications found in Hadoop Yarn have
        statuses failed or killed

    Notes
    -----
    This is a blocking operation. The program will wait until the retries are
    finished before exiting this function.
    """
    statuses = get_accumulo_appstates(namespace, log)
    log.info(f"MapReduce App Status: {statuses}")

    if len(statuses) == baseline_num_apps:
        raise RuntimeError("Never got a new Yarn application.")
    elif any(status != 'FINISHED' for status in statuses):
        raise RuntimeError("One or more Yarn applications failed, meaning Ingest was not successful.")


def get_mapreduce_statuses(resp: str,
                           log: logging.Logger = logging.getLogger('ingest_interactions')) -> list:
    """
    Pulls out the status of each hadoop yarn application to and saves them into
    a list.

    Parameters
    ----------
    resp: str
        Response string of performing an exec command on a pod. Specifically the
        cmd 'yarn app -list -appStates ALL'.
    log: logging.Logger
        The logger object.

    Return
    ------
    statuses: list
        a list of strings representing the applications statuses.
    """
    resp = re.sub(' *', '', resp)
    df = pd.read_csv(StringIO(resp), sep='\t', skiprows=3, header=0)
    log.info(f'\n{df}')
    statuses = df.State.to_list()

    return statuses


def get_accumulo_appstates(namespace: str,
                           log: logging.Logger = logging.getLogger('ingest_interactions')) -> list:
    """
    Gets all the states of the accumulo jobs found
    Parameters
    ----------
    namespace: str
        the kubernetes namespace to perform the actions for
    log: logging.Logger
        The logger object.

    Return
    ------
    a list of the statuses found from accumulo.
    """
    cmd = 'yarn application -list -appStates ALL'
    resp = pods.get_specific_pod(pods.yarn_rm_info, namespace).execute_cmd(cmd)
    return get_mapreduce_statuses(resp, log)


def check_for_file(filename: str, namespace: str,
                   log: logging.Logger = logging.getLogger('ingest_interactions')) -> bool:
    """
    Checks whether or not the given filename exists within the HDFS pod for datawave.

    Parameters
    ----------
    filename: str
        name of what file to check for.
    namespace: str
        the namespace to interact with.
    log: logging.Logger
        The logger object.

    Return
    ------
    A boolean indicating whether the file was found or not.
    """
    # Check file got copied to pod
    cmd = "ls tmp"
    log.info("Checking the test data file got copied to pod...")
    resp = pods.get_specific_pod(pods.hdfs_nn_info, namespace).execute_cmd(cmd)
    log.debug(resp)
    return filename in resp


def copy_file_to_pod(src_file: str, data_type: str,
                     log: logging.Logger = logging.getLogger('ingest_interactions'),
                     namespace: str = pods.namespace):
    """
    Copies the specified file into the HDFS DataWave for DataWave ingest to do its job.
    If the file is already found on the pod or fails to copy to the pod, the
    ingest script will not continue.

    Parameters
    ----------
    src_file: str
        the full path to the data file to be copied
    data_type: str
        The name of the data type
    log: logging.Logger
        The logger object.
    namespace: str
        the kubernetes namespace to perform the actions for
    """
    filename = Path(src_file).name
    if check_for_file(filename, namespace, log):
        log.warning("Data file was already found in tmp of pod, assuming data has already been loaded. Not proceeding.")
        sys.exit(0)
    cmd = [
        'kubectl',
        'cp',
        '-n',
        namespace,
        src_file,
        f"{pods.get_specific_pod(pods.hdfs_nn_info, namespace).podname}:/tmp/{filename}"
    ]

    log.debug(cmd)
    log.info("Running kubectl copy...")
    proc = subprocess.run(cmd)
    log.info(proc)

    if not check_for_file(filename, namespace, log):
        log.warning("Test data file was not found inside hadoop pod. Cannot continue with ingest script.")
        sys.exit(1)

    # copy local pod file to hdfs
    cmd = f'hdfs dfs -put /tmp/{filename} hdfs://hdfs-nn:9000/data/{data_type}'
    log.info("Running copy into HDFS...")
    resp = pods.get_specific_pod(pods.hdfs_nn_info, namespace).execute_cmd(cmd)
    log.info(resp)
    log.info("copy into HDFS complete...")


def check_for_required_cmds(cmds_to_check: list[str] = ['kubectl'],
                            log: logging.Logger = logging.getLogger('ingest_interactions')):
    """
    Checks for any external commands we utilize.

    Parameters
    ----------
    log: logging.Logger
        the logger object.

    Notes
    -----
    If any are not found, will exit the script after displaying an error.
    """
    if any(shutil.which(cmd) is None for cmd in cmds_to_check):
        log.critical(f"Cannot find one of the following: {cmds_to_check}. "
                     + "Please verify installations and try again.")
        sys.exit(1)


def main(args):
    log = setup_logger("ingest_interactions", log_level=args.log_level)
    check_for_required_cmds()

    if args.file:
        num_of_apps_pre = len(get_accumulo_appstates(args.namespace))
        copy_file_to_pod(src_file=args.file, data_type=args.data_type, namespace=args.namespace)

        # got a new job, time to check status.
        # Check Hadoop yarn for any failed or running applications
        check_app_statuses(num_of_apps_pre, args.namespace)
    else:
        get_accumulo_appstates(args.namespace, log)


@click.command('ingest', epilog="Requires the following to be installed. [kubectl]")
@click.option('-f', '--file', type=File(exists=True, file_type='.json'),
              help="The data file to ingest into DataWave. Must be a json file.")
@click.option('-d', '--data-type', type=str, help="The type of data within the data file.\n\n", cls=depends_on('file'))
@click.option('-n', '--namespace', type=str, default='dev-datawave', help="The kubernetes namespace to interact with.")
@click.option("--log-level", type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], case_sensitive=False),
              default="INFO", help="The level of logging details you want displayed.", show_default=True)
def ingest(**kwargs):
    """Handles the ingest to Datawave.

        \b
        Default Behavior: Displays the states of ingest jobs.
        If a file is provided, that file will be ingested instead.
            Note: If a file is provided, a datatype must also be specified.
    """
    main(SimpleNamespace(**kwargs))


if __name__ == "__main__":
    ingest(sys.argv[1:])