import json
import pytest
from pathlib import Path
from types import SimpleNamespace
from datawave_cli.dictionary_interactions import DictionaryInteractions


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
def dictionary_interactions(mock_args, mocker):
    mock_logger = mocker.Mock()
    return DictionaryInteractions(mock_args, log=mock_logger)


def load_test_data(which):
    json_file = Path(__file__).resolve().parent / which

    with open(json_file, 'r') as file:
        return json.load(file)


def test_get_pod_ip(dictionary_interactions, mocker):
    """Test that get_pod_ip grabs from the correct pod"""
    mock_pods = mocker.patch('datawave_cli.dictionary_interactions.pods')

    mock_pods.get_specific_pod.return_value.pod_ip = "127.0.0.1"

    pod_ip = dictionary_interactions.get_pod_ip()

    mock_pods.get_specific_pod.assert_called_once_with(mock_pods.web_dictionary_info, dictionary_interactions.namespace)
    assert pod_ip == "127.0.0.1"


@pytest.mark.parametrize(
    "file_provided",
    [
        ('output_file'),
        (None),
    ],
    ids=["Save to File", "Display in Log"]
)
def test_get_dictionary(dictionary_interactions, mocker, file_provided):
    """Test that get_dictionary either saves to file or displays data based on file argument"""

    mock_requests_get = mocker.patch('requests.get')
    mock_response = mocker.Mock(status_code=200)
    mock_response.json.return_value = {
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
            }
        ]
    }
    mock_requests_get.return_value = mock_response

    mock_parse_response = mocker.patch.object(dictionary_interactions, 'parse_response', return_value=['parsed_data'])

    mock_output_dictionary = mocker.patch.object(dictionary_interactions, 'output_dictionary')

    if file_provided:
        mock_open = mocker.mock_open()
        mocker.patch("builtins.open", mock_open)
        mock_partial_writer = mocker.Mock()
        mock_partial = mocker.patch('datawave_cli.dictionary_interactions.partial', return_value=mock_partial_writer)

        dictionary_interactions.get_dictionary('auths', 'types', file_provided)

        mock_open.assert_called_once_with(file_provided, 'a')

        mock_partial.assert_called_once_with(print, file=mock_open())
        mock_output_dictionary.assert_called_once_with(mock_partial_writer, ['parsed_data'])
    else:
        dictionary_interactions.get_dictionary('auths', 'types', None)
        mock_output_dictionary.assert_called_once_with(dictionary_interactions.log.info, ['parsed_data'])

    mock_requests_get.assert_called_once_with(
        f"{dictionary_interactions.base_url}/dictionary/data/v1/",
        data={'auths': 'auths', 'dataTypeFilters': 'types'},
        cert=dictionary_interactions.cert,
        headers=dictionary_interactions.headers,
        verify=False
    )

    mock_parse_response.assert_called_once_with(mock_response)


def test_parse_response(dictionary_interactions, mocker):
    """Test that parse_response correctly parses the dictionary fields"""
    mock_response = mocker.Mock()
    mock_response.json.return_value = {
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


@pytest.mark.parametrize(
    "fields, expected_header, expected_row_split, expected_rows",
    [(case['fields'], case['expected_header'], case['expected_row_split'], case['expected_rows'])
     for case in load_test_data('resources/test_dict_data.json')],
    ids=[
        'One row',
        'Two rows long description',
        'Empty fields'
    ]
)
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


@pytest.mark.parametrize(
    "fields, expected_header, expected_row_split, expected_rows",
    [(case['fields'], case['expected_header'], case['expected_row_split'], case['expected_rows'])
     for case in load_test_data('resources/test_dict_data.json')][:-1],
    ids=[
        'One row',
        'Two rows long description'
    ]
)
def test_output_dictionary(dictionary_interactions, mocker, fields, expected_header, expected_row_split, expected_rows):
    """Tests that `output_dictionary` is correctly calling the writer function"""
    mock_writer = mocker.Mock()

    mock_formatter = mocker.patch.object(dictionary_interactions, 'format_dictionary',
                                         return_value=[expected_header, expected_row_split, expected_rows])

    dictionary_interactions.output_dictionary(mock_writer, fields)

    mock_formatter.assert_called_once_with(fields)
    mock_writer.assert_has_calls([
        mocker.call(expected_header),
        mocker.call(expected_row_split),
        *[mocker.call(row) for row in expected_rows]
    ], any_order=False)
