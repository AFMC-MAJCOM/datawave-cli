import json
import pytest
from pathlib import Path
from types import SimpleNamespace

from requests.exceptions import HTTPError, JSONDecodeError, Timeout

from datawave_cli.dictionary_interactions import DictionaryInteractions, main
from tests.utils import ParamLoader


param_loader = ParamLoader('resources/dict_parameters.json')


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
        view=False,
        auths='fake_auths',
        data_types='fake_type'
    )
    return args


@pytest.fixture
def dictionary_interactions(mock_args, mocker):
    mock_logger = mocker.Mock()
    return DictionaryInteractions(mock_args, log=mock_logger)


def load_test_data(which):
    json_file = Path(__file__).resolve().parent / which

    with open(json_file, 'r') as file:
        return json.load(file)


def test_get_dictionary(dictionary_interactions, mocker):
    """Test that get_dictionary correctly queries datawave"""
    mock_requests_get = mocker.patch('requests.get')
    mock_response = mocker.Mock(status_code=200)
    mock_response.json.return_value = 'Mock Json Response'
    mock_requests_get.return_value = mock_response

    resp = dictionary_interactions.get_dictionary('auths', 'types')

    mock_requests_get.assert_called_once_with(
        f"{dictionary_interactions.base_url}/dictionary/data/v1/",
        data={'auths': 'auths', 'dataTypeFilters': 'types'},
        cert=dictionary_interactions.cert,
        headers=dictionary_interactions.headers,
        verify=False
    )
    assert resp == 'Mock Json Response'


@pytest.mark.parametrize(
    "mock_response_status, mock_exception",
    [
        (200, JSONDecodeError("Expecting value", "doc", 0)),
        (None, Timeout("The request timed out")),
        (403, HTTPError("403 Client Error: Forbidden for url")),
        (500, HTTPError("500 Server Error: Internal Server Error for url")),
    ],
    ids=["JSONDecodeError", "Timeout", "HTTPError_403", "HTTPError_500"]
)
def test_get_dictionary_errors(dictionary_interactions, mocker, mock_response_status, mock_exception):
    """Test get_dictionary with various error responses."""
    mock_requests_get = mocker.patch('requests.get')
    mocker.patch('datawave_cli.dictionary_interactions.log_http_response')

    mock_response = mocker.Mock()
    mock_response.status_code = mock_response_status

    match mock_exception:
        case JSONDecodeError():
            mock_response.json.side_effect = mock_exception
        case HTTPError():
            mock_response.raise_for_status.side_effect = mock_exception
        case _:
            mock_requests_get.side_effect = mock_exception

    mock_requests_get.return_value = mock_response

    with pytest.raises(RuntimeError, match="Invalid response from dictionary request"):
        dictionary_interactions.get_dictionary('auths', 'types')

    if not isinstance(mock_exception, Timeout):
        mock_requests_get.assert_called_once_with(
            f"{dictionary_interactions.base_url}/dictionary/data/v1/",
            data={'auths': 'auths', 'dataTypeFilters': 'types'},
            cert=dictionary_interactions.cert,
            headers=dictionary_interactions.headers,
            verify=False
        )


def test_parse_response(dictionary_interactions, mocker):
    """Test that parse_response correctly parses the dictionary fields"""
    mock_response = {
        'MetadataFields': [
            {
                'fieldName': 'field1',
                'dataType': 'string',
                'forwardIndexed': True,
                'reverseIndexed': False,
                'Types': 'text',
                'Descriptions': 'Test field',
                'indexOnly': False,
                'normalized': True,
                'tokenized': True,
                'lastUpdated': '2023-01-01'
            },
            {
                'fieldName': 'field2',
                'dataType': 'string',
                'forwardIndexed': True,
                'reverseIndexed': False,
                'Types': 'text',
                'Descriptions': 'Test field',
                'indexOnly': False,
                'normalized': True,
                'tokenized': True,
                'lastUpdated': '2023-01-01'
            }
        ]
    }

    expected_output = [
        {
            "name": "field1",
            "Data Type": "string",
            "Forward Indexed": True,
            "Reversed Indexed": False,
            "Types": "text",
            "Tokenized": True,
            "Normalized": True,
            "Index Only": False,
            "Descriptions": "Test field",
            "Last Updated": "2023-01-01"
        },
        {
            "name": "field2",
            "Data Type": "string",
            "Forward Indexed": True,
            "Reversed Indexed": False,
            "Types": "text",
            "Tokenized": True,
            "Normalized": True,
            "Index Only": False,
            "Descriptions": "Test field",
            "Last Updated": "2023-01-01"
        }
    ]

    result = dictionary_interactions.parse_response(mock_response)

    assert result == expected_output


@param_loader.parametrize
def test_format_dictionary(dictionary_interactions, fields, expected_header, expected_row_split, expected_rows):
    """Tests that `format_dictionary` correctly formats the fields into header, split, and rows."""
    header, row_split, rows = dictionary_interactions.format_dictionary(fields)
    assert header == expected_header
    assert row_split == expected_row_split
    assert rows == expected_rows

    if fields is None:
        dictionary_interactions.log.warning.assert_called_with('No fields provided, returning Nones')


def test_output_dictionary_no_fields(dictionary_interactions, mocker):
    """Test that output_dictionary logs a warning and returns when fields are empty."""
    mock_writer = mocker.Mock()

    result = dictionary_interactions.output_dictionary(mock_writer, [])

    dictionary_interactions.log.warning.assert_called_once_with("No fields to display")
    assert result is None
    mock_writer.assert_not_called()


@param_loader.parametrize
def test_output_dictionary(dictionary_interactions, mocker, fields, expected_header, expected_row_split, expected_rows):
    """Tests that `output_dictionary` is correctly calling the writer function"""
    mock_writer = mocker.Mock()

    mock_formatter = mocker.patch.object(dictionary_interactions, 'format_dictionary',
                                         return_value=[expected_header, expected_row_split, expected_rows])

    res = dictionary_interactions.output_dictionary(mock_writer, fields)

    if fields is None:
        dictionary_interactions.log.warning.assert_called_with('No fields to display')
        assert res is None
        return

    mock_formatter.assert_called_once_with(fields)
    mock_writer.assert_has_calls([
        mocker.call(expected_header),
        mocker.call(expected_row_split),
        *[mocker.call(row) for row in expected_rows]
    ], any_order=False)


@pytest.mark.parametrize(
    "output",
    [
        ('output_file'),
        (None),
    ],
    ids=["Save to File", "Display in Log"]
)
def test_main_routing(mock_args, output, mocker):
    """Tests that the main executes correctly and routes based on the `args.output` variable."""
    mock_args.output = output
    mock_setup_logger = mocker.patch('datawave_cli.dictionary_interactions.setup_logger',
                                     return_value=mocker.Mock())
    mock_di = mocker.Mock()
    mock_dictionary_interactions = mocker.patch('datawave_cli.dictionary_interactions.DictionaryInteractions',
                                                return_value=mock_di)
    mock_get_dictionary = mocker.patch.object(mock_di, 'get_dictionary',
                                              return_value={'Mocked': 'json', 'response': 'thing'})
    mock_parse_response = mocker.patch.object(mock_di, 'parse_response',
                                              return_value=[{'Mocked': 'Fields'}])
    mock_output_dictionary = mocker.patch.object(mock_di, 'output_dictionary')

    if output:
        mock_open = mocker.mock_open()
        mocker.patch("builtins.open", mock_open)
        mock_partial_writer = mocker.Mock()
        mock_partial = mocker.patch('datawave_cli.dictionary_interactions.partial', return_value=mock_partial_writer)

    fields = main(mock_args)

    mock_setup_logger.assert_called_once_with('dictionary_interactions', log_level=mock_args.log_level)
    mock_dictionary_interactions.assert_called_once_with(mock_args, mock_setup_logger.return_value)
    mock_get_dictionary.assert_called_once_with(mock_args.auths, mock_args.data_types)
    mock_parse_response.assert_called_once_with(mock_get_dictionary.return_value)

    if output is None:
        mock_output_dictionary.assert_called_once_with(mock_di.log.info,
                                                       mock_parse_response.return_value)
    else:
        mock_open.assert_called_once_with(output, 'w')
        mock_partial.assert_called_once_with(print, file=mock_open.return_value)
        mock_output_dictionary.assert_called_once_with(mock_partial_writer, mock_parse_response.return_value)

    assert fields == mock_parse_response.return_value