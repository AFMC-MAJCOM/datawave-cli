import pytest
from requests.exceptions import HTTPError, JSONDecodeError, Timeout

from types import SimpleNamespace

from datawave_cli.authorizations_interactions import AuthorizationInteractions, main
from tests.utils import create_mock_requests_get


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
def authorization_interactions(mock_args, mocker):
    mock_logger = mocker.Mock()
    return AuthorizationInteractions(mock_args, log=mock_logger)


@pytest.fixture
def mock_log_http_response(mocker):
    mock_log_http_response = mocker.patch('datawave_cli.authorizations_interactions.log_http_response')
    return mock_log_http_response


@pytest.fixture
def test_user():
    return {
        "proxiedUsers": [
            {
                "name": "test.user",
                "auths": ["auth1", "auth2"],
                "roles": ["AuthorizedUser"]
            }
        ]
    }


def test_whoami(mocker, authorization_interactions, mock_log_http_response, test_user):
    """Tests the whoami function call to validate we are getting correct responses
    and performing correct logic."""
    mock_requests_get, mock_response = create_mock_requests_get(mocker, None, 200, '')
    mock_response.json.return_value = test_user

    resp = authorization_interactions.authorization_whoami()

    mock_requests_get.assert_called_once_with(
        f"{authorization_interactions.base_url}/authorization/v1/whoami",
        cert=authorization_interactions.cert,
        headers=authorization_interactions.headers,
        verify=False
    )

    mock_log_http_response.assert_called_once_with(mock_response, authorization_interactions.log)
    authorization_interactions.log.info.assert_any_call("Getting the authorization details for my cert from DW...")

    assert resp['proxiedUsers'][0]['name'] == test_user['proxiedUsers'][0]['name']
    assert resp['proxiedUsers'][0]['auths'] == test_user['proxiedUsers'][0]['auths']
    assert resp['proxiedUsers'][0]['roles'] == test_user['proxiedUsers'][0]['roles']


@pytest.mark.parametrize(
    ("mock_status_code", "mock_exception"),
    [
        (403, HTTPError("403 Client Error: Forbidden for url")),
        (500, HTTPError("500 Server Error: Internal Server Error for url")),
        (None, Timeout("The request timed out"))
    ],
    ids=[
        'Forbidden',
        'Internal Server Error',
        'Timeout',
    ]
)
def test_whoami_errors(mock_status_code, mock_exception, mocker, authorization_interactions):
    """Tests the whoami with its various potential errors and the error handling."""
    mock_requests_get, mock_response = create_mock_requests_get(mocker, mock_exception, mock_status_code, None)

    with pytest.raises(RuntimeError, match='A bad response from the endpoint whoami was found'):
        authorization_interactions.authorization_whoami()

    mock_requests_get.assert_called_once_with(
        f"{authorization_interactions.base_url}/authorization/v1/whoami",
        cert=authorization_interactions.cert,
        headers=authorization_interactions.headers,
        verify=False
    )


def test_evict_users_success(mocker, authorization_interactions, mock_log_http_response):
    """Tests evict_users is called correctly and we the returned is what we eppect."""
    mock_requests_get, mock_response = create_mock_requests_get(mocker, None, 200, "all entries evicted")

    resp = authorization_interactions.authorization_evict_users()

    mock_requests_get.assert_called_once_with(
        f"{authorization_interactions.base_url}/authorization/v1/admin/evictAll",
        cert=authorization_interactions.cert,
        headers=authorization_interactions.headers,
        verify=False
    )

    mock_log_http_response.assert_called_once_with(mock_response, authorization_interactions.log)
    authorization_interactions.log.info.assert_any_call("Requesting all users to be evicted from DW...")

    assert resp == mock_response
    assert resp.text == "all entries evicted"
    assert resp.status_code == 200


@pytest.mark.parametrize(
    ("mock_status_code", "mock_resp_text", "mock_exception"),
    [
        (403, '', HTTPError("403 Client Error: Forbidden for url")),
        (500, '', HTTPError("500 Server Error: Internal Server Error for url")),
        (None, None, Timeout("The request timed out"))
    ],
    ids=[
        'Forbidden',
        'Internal Server Error',
        'Timeout',
    ]
)
def test_evict_users_errors(mock_status_code, mock_resp_text, mock_exception, mocker, authorization_interactions):
    """Tests the evict_users handles errors properly."""
    mock_requests_get, mock_response = create_mock_requests_get(mocker, mock_exception, mock_status_code,
                                                                mock_resp_text)
    with pytest.raises(RuntimeError, match="An error occurred while requesting to evict all users"):
        authorization_interactions.authorization_evict_users()

    mock_requests_get.assert_called_once_with(
        f"{authorization_interactions.base_url}/authorization/v1/admin/evictAll",
        cert=authorization_interactions.cert,
        headers=authorization_interactions.headers,
        verify=False
    )