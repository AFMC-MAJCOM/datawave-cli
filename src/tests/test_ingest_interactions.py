import pytest

from datawave_cli.utilities import pods
from datawave_cli.utilities.utilities import Retry
from datawave_cli.ingest_interactions import (check_app_statuses, get_mapreduce_statuses, get_accumulo_appstates,
                                              check_for_file, copy_file_to_pod, check_for_required_cmds, main)


@pytest.mark.parametrize(
    "statuses, baseline_num_apps, expected_exception, error_text",
    [
        (['FINISHED'], 0, None, None),
        (['FINISHED', 'FINISHED'], 1, None, None),
        (['RUNNING'], 0, RuntimeError,
         "One or more Yarn applications failed, meaning Ingest was not successful."),
        (['FINISHED', 'FAILED'], 1, RuntimeError,
         "One or more Yarn applications failed, meaning Ingest was not successful."),
        (['FINISHED'], 1, RuntimeError,
         "Never got a new Yarn application.")
    ],
    ids=["Single finished", "Multiple finished", "Non-finished state", "Multiple with a fail", "No new application"]
)
def test_check_app_statuses(statuses, baseline_num_apps, expected_exception, error_text, mocker):
    mocker.patch.object(Retry, 'testing', True)

    log = mocker.Mock()
    mock_get_accumulo_appstates = mocker.patch(
        'datawave_cli.ingest_interactions.get_accumulo_appstates',
        return_value=statuses
    )
    if expected_exception:
        with pytest.raises(expected_exception, match=error_text):
            check_app_statuses(baseline_num_apps, "test-namespace", log)
    else:
        check_app_statuses(baseline_num_apps, "test-namespace", log)

    mock_get_accumulo_appstates.assert_called_once_with("test-namespace", log)


@pytest.mark.parametrize(
    "resp, expected_statuses",
    [
        ("\n\n\nappID\tState\n1\tFINISHED", ['FINISHED']),
        ("\n\n\nappID\tState\n1\tFAILED\n2\tFINISHED", ['FAILED', 'FINISHED']),
    ],
    ids=["Single app", "Multiple apps"]
)
def test_get_mapreduce_statuses(resp, expected_statuses, mocker):
    log = mocker.Mock()
    statuses = get_mapreduce_statuses(resp, log)
    assert statuses == expected_statuses


def test_get_accumulo_appstates(mocker):
    log = mocker.Mock()
    mock_pod = mocker.Mock()
    mock_pod.execute_cmd.return_value = "\n\n\nappID\tState\n1\tFINISHED\n2\tFAILED"
    mock_get_specific_pod = mocker.patch(
        'datawave_cli.ingest_interactions.pods.get_specific_pod',
        return_value=mock_pod
    )
    mock_get_mapreduce_statuses = mocker.patch(
        'datawave_cli.ingest_interactions.get_mapreduce_statuses',
        return_value=["FINISHED", "FAILED"]
    )

    statuses = get_accumulo_appstates("test-namespace", log)

    assert statuses == ["FINISHED", "FAILED"]
    mock_get_specific_pod.assert_called_once_with(pods.yarn_rm_info, "test-namespace")
    mock_pod.execute_cmd.assert_called_once()
    mock_get_mapreduce_statuses.assert_called_once_with(mock_pod.execute_cmd.return_value, log)


@pytest.mark.parametrize(
    "resp, expected_result",
    [
        ("tmp/testfile.txt", True),
        ("", False),
    ],
    ids=["File exists", "File does not exist"]
)
def test_check_for_file(resp, expected_result, mocker):
    log = mocker.Mock()
    mock_pod = mocker.Mock()
    mock_pod.execute_cmd.return_value = resp
    mock_get_specific_pod = mocker.patch(
        'datawave_cli.ingest_interactions.pods.get_specific_pod',
        return_value=mock_pod
    )

    result = check_for_file("testfile.txt", "test-namespace", log)
    assert result == expected_result
    mock_get_specific_pod.assert_called_once_with(pods.hdfs_nn_info, "test-namespace")
    mock_pod.execute_cmd.assert_called_once()


@pytest.mark.parametrize(
    'check_file_results, expected_warning, expected_exit_code',
    [
        ([False, True], None, None),
        ([True, None], "Data file was already found in tmp of pod, assuming data has already been loaded. "
         "Not proceeding.", 0),
        ([False, False], "Test data file was not found inside hadoop pod. Cannot continue with ingest script.", 1),
    ],
    ids=['Success', 'First check fails', 'Second Check fails']
)
def test_copy_file_to_pod(check_file_results, expected_warning, expected_exit_code, mocker):
    src_file = "/path/to/testfile.txt"
    filename = "testfile.txt"
    data_type = "datatype"

    mock_log = mocker.Mock()
    mock_pod = mocker.Mock()
    mock_get_specific_pod = mocker.patch(
        'datawave_cli.ingest_interactions.pods.get_specific_pod',
        return_value=mock_pod
    )
    mock_check_for_file = mocker.patch(
        'datawave_cli.ingest_interactions.check_for_file',
        side_effect=check_file_results
    )
    mock_subprocess_run = mocker.patch('datawave_cli.ingest_interactions.subprocess.run')
    mock_exit = mocker.patch("datawave_cli.ingest_interactions.sys.exit", side_effect=SystemExit)

    if expected_exit_code is not None:
        with pytest.raises(SystemExit):
            copy_file_to_pod(src_file, data_type, mock_log, "test-namespace")
        mock_log.warning.assert_called_once_with(expected_warning)
        mock_exit.assert_called_once_with(expected_exit_code)
        return
    else:
        copy_file_to_pod(src_file, data_type, mock_log, "test-namespace")
        mock_exit.assert_not_called()

    mock_get_specific_pod.assert_called_with(pods.hdfs_nn_info, "test-namespace")
    mock_check_for_file.assert_any_call(filename, "test-namespace", mock_log)
    mock_subprocess_run.assert_called_once_with([
        'kubectl', 'cp', '-n', 'test-namespace', src_file,
        f"{mock_pod.podname}:/tmp/{filename}"
    ])
    mock_pod.execute_cmd.assert_called_once_with(f'hdfs dfs -put /tmp/{filename} hdfs://hdfs-nn:9000/data/{data_type}')


@pytest.mark.parametrize(
    "cmds_to_check, which_side_effect, should_exit, expected_log",
    [
        (["kubectl"], ["/usr/bin/kubectl"], False, None),
        (["kubectl"], [None], True, "Cannot find one of the following: ['kubectl']. Please verify installations and "
         "try again."),
        (["kubectl", "hdfs"], ["/usr/bin/kubectl", "/usr/bin/hdfs"], False, None),
        (["kubectl", "hdfs"], ["/usr/bin/kubectl", None], True, "Cannot find one of the following: ['kubectl', 'hdfs']."
         " Please verify installations and try again.")
    ],
    ids=["single_command_success", "single_command_fail", "multiple_commands_success", "multiple_commands_fail"]
)
def test_check_for_required_cmds(cmds_to_check, which_side_effect, should_exit, expected_log, mocker):
    log = mocker.Mock()
    mocker.patch('datawave_cli.ingest_interactions.shutil.which', side_effect=which_side_effect)
    mocker.patch('datawave_cli.ingest_interactions.sys.exit', side_effect=SystemExit)

    if should_exit:
        with pytest.raises(SystemExit):
            check_for_required_cmds(cmds_to_check, log)
        log.critical.assert_called_once_with(expected_log)
    else:
        check_for_required_cmds(cmds_to_check, log)
        log.critical.assert_not_called()


@pytest.mark.parametrize(
    "args_file_present",
    [True, False],
    ids=["with_file", "without_file"]
)
def test_main_routing(args_file_present, mocker):
    args = mocker.Mock()
    args.file = "/path/to/file.txt" if args_file_present else None
    args.namespace = "test-namespace"
    args.data_type = "test-data-type"
    args.log_level = "DEBUG"

    mock_setup_logger = mocker.patch('datawave_cli.ingest_interactions.setup_logger', return_value=mocker.Mock())
    mock_check_for_required_cmds = mocker.patch('datawave_cli.ingest_interactions.check_for_required_cmds')
    mock_get_accumulo_appstates = mocker.patch('datawave_cli.ingest_interactions.get_accumulo_appstates',
                                               return_value=['FINISHED'])
    mock_copy_file_to_pod = mocker.patch('datawave_cli.ingest_interactions.copy_file_to_pod')
    mock_check_app_statuses = mocker.patch('datawave_cli.ingest_interactions.check_app_statuses')

    main(args)

    mock_setup_logger.assert_called_once_with("ingest_interactions", log_level=args.log_level)
    mock_check_for_required_cmds.assert_called_once()

    if args_file_present:
        mock_get_accumulo_appstates.assert_called_with(args.namespace)
        mock_copy_file_to_pod.assert_called_once_with(src_file=args.file, data_type=args.data_type,
                                                      namespace=args.namespace)
        mock_check_app_statuses.assert_called_once_with(len(mock_get_accumulo_appstates.return_value), args.namespace)
    else:
        mock_get_accumulo_appstates.assert_called_once_with(args.namespace, mock_setup_logger())
        mock_copy_file_to_pod.assert_not_called()
        mock_check_app_statuses.assert_not_called()