import pytest
import sys
from types import SimpleNamespace
from unittest.mock import Mock
from datawave_cli.base_interactions import BaseInteractions


class MockBaseInteractions(BaseInteractions):
    """Mock subclass to allow for instantiation of ABC"""
    log = Mock()

    @property
    def pod_info(self):
        return "mock_pod_info"


@pytest.fixture
def mock_args():
    """Fixture to provide mock arguments for initializing BaseInteractions."""
    return SimpleNamespace(
        namespace="test-namespace",
        cert="test-cert.pem",
        key=None,
        localhost=False,
        ip=False,
        url=None,
        header=[("Authorization", "Bearer test-token")]
    )


@pytest.fixture
def mock_args_with_key(mock_args):
    """Fixture for arguments with both cert and key."""
    mock_args.key = "test-key.pem"
    return mock_args


@pytest.fixture
def mock_args_with_localhost(mock_args):
    """Fixture for arguments with localhost set to True."""
    mock_args.localhost = True
    return mock_args


@pytest.fixture
def mock_args_with_ip(mock_args, mocker):
    """Fixture for arguments with IP set to True."""
    mock_args.ip = True

    mock_pods = mocker.patch('datawave_cli.base_interactions.pods')
    mock_pods.get_specific_pod.return_value.pod_ip = "mocked-ip"
    return mock_args


@pytest.fixture
def mock_args_with_url(mock_args):
    """Fixture for arguments with a URL specified."""
    mock_args.url = "test-url.com"
    return mock_args


@pytest.mark.parametrize(
    "args_fixture, expected_cert",
    [
        ("mock_args", "test-cert.pem"),
        ("mock_args_with_key", ("test-cert.pem", "test-key.pem")),
    ],
    ids=['Init cert without key', 'Init cert with key']
)
def test_init_cert(args_fixture, expected_cert, request, mocker):
    """Test that cert is initialized correctly based on args."""
    args = request.getfixturevalue(args_fixture)
    mocker.patch('datawave_cli.base_interactions.BaseInteractions.init_base_url')
    interaction = MockBaseInteractions(args)
    assert interaction.cert == expected_cert


@pytest.mark.parametrize(
    "args_fixture, expected_url",
    [
        ("mock_args_with_localhost", "https://localhost:8443"),
        ("mock_args_with_ip", "https://mocked-ip:8443"),
        ("mock_args_with_url", "https://test-url.com")
    ],
    ids=['Localhost', 'IP', 'URL']
)
def test_init_base_url(args_fixture, expected_url, request, mocker):
    """Test that base_url is set correctly based on args."""
    args = request.getfixturevalue(args_fixture)
    interaction = MockBaseInteractions(args)

    assert interaction.base_url == expected_url


def test_exit_on_missing_url(mock_args, mocker):
    """Test that the application exits when URL is missing and localhost/IP are False."""
    mock_sys_exit = mocker.patch.object(sys, "exit")

    interaction = MockBaseInteractions(mock_args)

    interaction.log.critical.assert_called_once_with("URL is none, cannot continue.")
    mock_sys_exit.assert_called_once_with(1)


def test_init_headers(mock_args_with_url):
    """Test that headers are initialized correctly."""
    expected_headers = {"Authorization": "Bearer test-token"}
    interaction = MockBaseInteractions(mock_args_with_url)
    assert interaction.headers == expected_headers


def test_get_pod_ip(mock_args, mocker):
    """Test that get_pod_ip grabs from correct pod"""
    mock_args.ip = True
    mock_pods = mocker.patch('datawave_cli.base_interactions.pods')
    mock_pods.get_specific_pod.return_value.pod_ip = "mocked-ip"
    interaction = MockBaseInteractions(mock_args)

    mock_pods.get_specific_pod.assert_called_once_with('mock_pod_info', mock_args.namespace)
    pod_ip = interaction.get_pod_ip()
    assert pod_ip == "mocked-ip"