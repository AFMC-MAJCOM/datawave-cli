import pytest
from types import SimpleNamespace
from datawave_cli.accumulo_interactions import AccumuloInteractions, main


@pytest.fixture
def mock_args():
    args = SimpleNamespace(
        url="https://test.com",
        ip=False,
        localhost=False,
        cert="fake_cert",
        key="fake_key",
        header=[("Authorization", "Bearer fake_token")],
        namespace="test_namespace",
        log_level="INFO",
        view=False
    )
    return args


@pytest.fixture
def accumulo_interactions(mock_args, mocker):
    mock_logger = mocker.Mock()
    return AccumuloInteractions(mock_args, log=mock_logger)


def test_reload_accumulo_cache(accumulo_interactions, mocker):
    """Tests that reloading the cache hits the correct endpoint and correctly outputs the logs"""
    mock_requests_get = mocker.patch('requests.get')
    mock_response = mocker.Mock()
    mock_response.status_code = 200
    mock_requests_get.return_value = mock_response

    mock_log_http_response = mocker.patch('datawave_cli.accumulo_interactions.log_http_response')

    accumulo_interactions.reload_accumulo_cache()

    mock_requests_get.assert_called_once_with(
        f"{accumulo_interactions.base_url}/DataWave/Common/AccumuloTableCache/reload/datawave.metadata",
        cert=accumulo_interactions.cert,
        headers=accumulo_interactions.headers,
        verify=False
    )
    mock_log_http_response.assert_called_once_with(mock_response, accumulo_interactions.log)
    accumulo_interactions.log.info.assert_any_call("Reloading the accumulo cache...")
    accumulo_interactions.log.info.assert_any_call("Successfully requested a reload.")


def test_view_accumulo_cache(accumulo_interactions, mocker):
    """Tests that viewing the cache works as expected and returns the results"""
    mock_requests_get = mocker.patch('requests.get')
    mock_response = mocker.Mock(text="Cache Test Data")
    mock_requests_get.return_value = mock_response

    mock_log_http_response = mocker.patch('datawave_cli.accumulo_interactions.log_http_response')

    response_text = accumulo_interactions.view_accumulo_cache()

    mock_requests_get.assert_called_once_with(
        f"{accumulo_interactions.base_url}/DataWave/Common/AccumuloTableCache/",
        cert=accumulo_interactions.cert,
        headers=accumulo_interactions.headers,
        verify=False
    )
    mock_log_http_response.assert_called_once_with(mock_response, accumulo_interactions.log)
    accumulo_interactions.log.info.assert_any_call("Viewing the accumulo cache...")

    assert response_text == "Cache Test Data"


@pytest.mark.parametrize(
    "view_flag, expected_log_call",
    [
        (True, "Cache View Data"),
        (False, None)
    ],
    ids=[
        'View Cache',
        'Reload Cache'
    ]
)
def test_main(view_flag, expected_log_call, mock_args, mocker):
    """Tests that main routes based on arg.view_flag correctly"""
    mock_logger = mocker.patch('datawave_cli.accumulo_interactions.setup_logger')
    mock_logger.return_value = mocker.Mock()

    mock_args.view = view_flag

    instance = mocker.patch('datawave_cli.accumulo_interactions.AccumuloInteractions', autospec=True).return_value
    instance.view_accumulo_cache.return_value = expected_log_call

    main(mock_args)

    if view_flag:
        instance.view_accumulo_cache.assert_called_once()
        mock_logger.return_value.info.assert_called_with(expected_log_call)
    else:
        instance.reload_accumulo_cache.assert_called_once()
        mock_logger.return_value.info.assert_not_called()
