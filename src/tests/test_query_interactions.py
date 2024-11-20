import base64
import re
from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from datawave_cli.query_interactions import QueryParams, QueryConnection, QueryInteractions
from tests.utils import ParamLoader


param_loader = ParamLoader('resources/query_parameters.json')


def test_query_params():
    params = QueryParams(query_name="test_name", query="test_query", auths="test_auths")

    assert params.column_visibility == "N/A"
    assert params.page_size == 5
    assert params.begin == "19700101"
    assert params.end == "20990101"

    expected_dict = {
        "queryName": "test_name",
        "columnVisibility": "N/A",
        "pagesize": 5,
        "begin": "19700101",
        "end": "20990101",
        "query": "test_query",
        "auths": "test_auths",
        "query.syntax": "JEXL"
    }
    assert params.get() == expected_dict


class TestQueryConnection:
    @pytest.fixture
    def query_params(self):
        """Fixture to provide a QueryParams instance for testing."""
        return QueryParams(query_name="test_query", query="SELECT * FROM table", auths="auth")

    @pytest.fixture
    def query_conn(self, query_params, mocker):
        """Fixture to create a QueryConnection instance."""
        return QueryConnection(base_url="https://testserver", cert="path/to/cert",
                               query_params=query_params, log=mocker.Mock())

    def test_initialization(self, query_params, mocker):
        """Test that the QueryConnection initializes correctly."""
        mock_setup_logger = mocker.patch("datawave_cli.query_interactions.setup_logger")
        query_conn = QueryConnection(base_url="https://testserver", cert="path/to/cert", query_params=query_params)

        assert query_conn.base_url == "https://testserver"
        assert query_conn.cert == "path/to/cert"
        assert query_conn.query_params.query_name == "test_query"
        assert query_conn.results_count == 0
        mock_setup_logger.assert_called_once()

    @pytest.mark.parametrize(
        ("endpoint_property", "expected_error_message"),
        [
            ("next_endpoint", "Query UUID not set"),
            ("close_endpoint", "Query UUID not set"),
        ],
        ids=['Next Endpoint', 'Close Endpoint']
    )
    def test_endpoint_without_quuid(self, query_conn, endpoint_property, expected_error_message):
        """Test that accessing endpoint properties without quuid raises an error."""
        with pytest.raises(ValueError, match=expected_error_message):
            _ = getattr(query_conn, endpoint_property)

    @pytest.mark.parametrize(
        ("status_code", "json_response", "expected_quuid"),
        [
            (200, {"Result": "12345"}, "12345"),
            (500, {}, None),
        ],
        ids=["Success", "Failure"]
    )
    def test_enter(self, status_code, json_response, expected_quuid, query_conn, query_params, mocker):
        """Test __enter__ method with both successful and failed responses."""
        mocker.patch('datawave_cli.query_interactions.log_http_response')
        mocker.patch.object(QueryConnection, '__exit__')
        mock_response = mocker.Mock(status_code=status_code)
        mock_response.json.return_value = json_response
        mock_post = mocker.patch("datawave_cli.query_interactions.requests.post", return_value=mock_response)

        if not expected_quuid:
            with pytest.raises(RuntimeError, match="Create endpoint came back with non-200 response. 500"):
                with query_conn:
                    pass
            query_conn.log.error.assert_called()
        else:
            with query_conn as conn:
                assert conn.quuid == expected_quuid
                assert conn.open is True

        mock_post.assert_called_once_with(
            f"{query_conn.base_url}/{query_conn.create_endpoint}",
            data=query_params.get(),
            cert="path/to/cert",
            headers=None,
            verify=False
        )

    @pytest.mark.parametrize(
        ("results_count", "expected_log"),
        [
            (0, "No results found!"),
            (50, "Total results retrieved: 50"),
        ],
        ids=["No results", "Has results"]
    )
    def test_exit(self, results_count, expected_log, query_conn, mocker):
        """Test the __exit__ method calls the close endpoint."""
        mocker.patch.object(QueryConnection, '__enter__')
        query_conn.quuid = "12345"
        query_conn.open = True
        query_conn.results_count = results_count
        mock_get = mocker.patch("datawave_cli.query_interactions.requests.get")

        with query_conn:
            # Exiting the context will trigger __exit__
            pass

        assert query_conn.open is False
        mock_get.assert_called_once_with(
            f"{query_conn.base_url}/{query_conn.close_endpoint}",
            cert="path/to/cert",
            headers=None,
            verify=False
        )
        query_conn.log.info.assert_called_with(expected_log)

    def test_iter(self, query_conn):
        """Test the __iter__ method."""
        query_conn.open = True
        assert iter(query_conn) == query_conn

    def test_iter_raises_error(self, query_conn):
        """Test that __iter__ raises RuntimeError if query has not been started."""
        query_conn.open = False
        with pytest.raises(RuntimeError, match="Query has not been started"):
            iter(query_conn)

    @pytest.mark.parametrize(
        ("status_code", "json_data"),
        [
            (200, {"ReturnedEvents": 10, "Results": "some data"}),
            (404, None),
        ],
        ids=["Success", "Stop Iteration"]
    )
    def test_next(self, status_code, json_data, query_conn, mocker):
        """Test __next__ behavior with successful fetch and StopIteration."""
        mocker.patch('datawave_cli.query_interactions.log_http_response')
        query_conn.quuid = "12345"
        query_conn.open = True

        mock_response = mocker.Mock()
        mock_response.status_code = status_code
        mock_response.json.return_value = json_data
        mock_get = mocker.patch("datawave_cli.query_interactions.requests.get", return_value=mock_response)

        if not json_data:
            with pytest.raises(StopIteration):
                next(query_conn)
        else:
            result = next(query_conn)
            assert result == json_data
            assert query_conn.results_count == json_data["ReturnedEvents"]

        mock_get.assert_called_once_with(
            f"https://testserver/{query_conn.next_endpoint}",
            cert="path/to/cert",
            headers=None,
            verify=False
        )


class TestQueryInteractions:
    @pytest.fixture
    def mock_args(self):
        """Fixture to provide mock arguments for initializing BaseInteractions."""
        return SimpleNamespace(
            namespace="test-namespace",
            cert="test-cert.pem",
            key=None,
            localhost=False,
            ip=False,
            url='test-url.com',
            header=[("Authorization", "Bearer test-token")]
        )

    @pytest.fixture
    def query_interactions(self, mock_args, mocker):
        log = mocker.Mock()
        mocker.patch('datawave_cli.query_interactions.log_http_response')
        return QueryInteractions(mock_args, log)

    @pytest.mark.parametrize(
        ("output", "html"),
        [
            (None, False),
            ('output.txt', False),
            ('output.txt', True)
        ],
        ids=['Print Query', 'Save Query No HTML', 'Save Query With HTML']
    )
    def test_perform_query(self, output, html, query_interactions, mocker):
        """Test perform_query runs, returns results, and correctly routes to save or print."""
        args = SimpleNamespace(query_name='test_query', query="GENRES == 'Test'", auths='mock_auth',
                               filter='mock_dtype', output=output, decode_raw=False, html=html)
        query_params = QueryParams('test_query', "GENRES == 'Test'", 'mock_auth')
        expected_events = ['event1', 'event2']

        mock_connection = mocker.patch("datawave_cli.query_interactions.QueryConnection", autospec=True)
        mock_connection_instance = mock_connection.return_value.__enter__.return_value
        mock_connection_instance.__iter__.return_value = ["raw_event"]
        mock_connection_instance.query_params = query_params
        mock_connection_instance.results_count = len(expected_events)
        mock_connection_instance.cert = query_interactions.cert

        mock_parse_and_filter_results = mocker.patch.object(query_interactions, "parse_and_filter_results",
                                                            return_value=expected_events)
        mock_print_query = mocker.patch.object(query_interactions, "print_query")
        mock_save_query = mocker.patch.object(query_interactions, "save_query")
        mock_htmlify = mocker.patch('datawave_cli.query_interactions.htmlify')

        result = query_interactions.perform_query(args)

        assert result["events"] == expected_events
        assert result["metadata"]["Query"] == args.query
        assert result["metadata"]["Returned Events"] == len(expected_events)
        assert result["metadata"]["Auths"] == args.auths
        assert result["metadata"]["Cert"] == "test-cert"
        # Check timestamp is in MS
        result_timestamp = result["metadata"]["Unix Timestamp(ms)"]
        assert re.match(r'\d{13}', f'{result_timestamp}'), "Timestamp is not in milliseconds"
        # Check it is reasonably close to current time
        result_time = datetime.fromtimestamp(result_timestamp / 1e3)
        assert abs(result_time - datetime.now()) < timedelta(seconds=5), "Timestamp is outside expected range"

        mock_parse_and_filter_results.assert_called_once_with("raw_event", filter_on='mock_dtype')

        if output:
            mock_save_query.assert_called_once_with(result, args.output, args.decode_raw)
            if html:
                mock_htmlify.assert_called_once_with(args.output)
        else:
            mock_print_query.assert_called_once_with(result, args.decode_raw)

    @pytest.mark.parametrize(
        ("raw_events", "filter_on", "expected_result", "side_effect"),
        [
            ([{"field1": "value1", "field2": "value2"}], "field1", [{"field1": "value1"}], None),
            ([{"field1": "value1", "field2": "value2"}], "field3", [], KeyError('[field3] not found in any results!')),
        ],
        ids=["Success", "Key not found"]
    )
    def test_parse_and_filter_results(self, raw_events, filter_on, expected_result, side_effect, query_interactions,
                                      mocker):
        """Test parse_and_filter_results with valid filtering and missing keys."""
        mock_parse_results = mocker.patch.object(query_interactions, "parse_results", return_value=raw_events)
        mock_filter_results = mocker.patch.object(query_interactions, "filter_results", return_value=expected_result,
                                                  side_effect=side_effect)

        result = query_interactions.parse_and_filter_results(raw_events, filter_on=filter_on)

        assert result == expected_result
        mock_parse_results.assert_called_once_with(raw_events)
        mock_filter_results.assert_called_once_with(mock_parse_results.return_value, filter_on=filter_on)

        if side_effect:
            query_interactions.log.error.assert_called_once_with(side_effect)
            query_interactions.log.info.assert_called_once_with("Returning empty results")
        else:
            query_interactions.log.error.assert_not_called()
            query_interactions.log.info.assert_not_called()

    @param_loader.parametrize
    def test_parse_results(self, raw_events, expected_parsed, query_interactions):
        """Test parse_results parses the raw event format correctly."""
        result = query_interactions.parse_results(raw_events)
        assert result == expected_parsed

    @param_loader.parametrize
    def test_filter_results(self, results_in, filter_on, expected_output, should_raise, query_interactions):
        """Test filter_results filters keys correctly or raises KeyError if missing."""
        if should_raise:
            with pytest.raises(KeyError, match=re.escape(f"{[filter_on]} not found in any results!")):
                query_interactions.filter_results(results_in, filter_on)
        else:
            output = query_interactions.filter_results(results_in, filter_on)
            assert output == expected_output

    @pytest.mark.parametrize(
        "decode_raw, raw_value, expected_output",
        [
            (False, base64.b64encode(b"fake_binary_data"), "Contains raw data"),
            (True, base64.b64encode(b"fake_binary_data"), pd.DataFrame()),
        ],
        ids=["Decode False", "Decode True"]
    )
    def test_print_query(self, decode_raw, raw_value, expected_output, query_interactions, mocker):
        results = {
            "events": [
                {
                    "field1": "value1",
                    "RAWDATA_field": raw_value
                }
            ],
            "metadata": {
                "Returned Events": 1
            }
        }

        mock_print = mocker.patch("builtins.print")
        if decode_raw:
            mock_read_parquet = mocker.patch("datawave_cli.query_interactions.pd.read_parquet",
                                             return_value=expected_output)

        query_interactions.print_query(results, decode_raw=decode_raw)

        mock_print.assert_any_call("field1: value1")
        raw_print = "RAWDATA_field: " + (str(expected_output) if decode_raw else "Contains raw data")
        mock_print.assert_any_call(raw_print)
        mock_print.assert_any_call("Query returned: 1 events.")

        if decode_raw:
            # We can't just check that it was called with the buffer since buffer objects are unique.
            # So we're just gonna check that it was called once
            mock_read_parquet.assert_called_once()

            # Then we will check that the content in the BytesIO object matches decoded_data
            decoded_data = base64.b64decode(raw_value)
            actual_buffer = mock_read_parquet.call_args[0][0]
            assert isinstance(actual_buffer, BytesIO)
            assert actual_buffer.getvalue() == decoded_data

    @pytest.fixture
    def sq_setup(self, tmp_path):
        filename = tmp_path / "output.json"
        results = {"metadata": {"Returned Events": 1}, "events": [{"field1": "value1"}]}
        return SimpleNamespace(filename=filename, results=results)

    @pytest.fixture
    def sq_mocks(self, mocker):
        mock_open = mocker.patch("builtins.open", mocker.mock_open(), create=True)
        mock_json_dump = mocker.patch("json.dump")
        mock_path_mkdir = mocker.patch.object(Path, "mkdir")
        mock_path_exists = mocker.patch.object(Path, "exists", return_value=False)
        mock_path_rename = mocker.patch.object(Path, "rename")
        return SimpleNamespace(open=mock_open, json=mock_json_dump, mkdir=mock_path_mkdir, exists=mock_path_exists,
                               rename=mock_path_rename)

    def test_save_query_basic(self, query_interactions, sq_setup, sq_mocks):
        """Test basic save operation when the file does not exist and decode_raw is False."""

        query_interactions.save_query(sq_setup.results, str(sq_setup.filename), decode_raw=False)

        sq_mocks.json.assert_called_once_with(sq_setup.results, sq_mocks.open(), indent=2)
        sq_mocks.mkdir.assert_called_once_with(parents=True, exist_ok=True)

    def test_save_query_rename(self, query_interactions, sq_setup, sq_mocks,):
        """Test saving when file exists, triggering renaming."""
        sq_mocks.exists.return_value = True

        query_interactions.save_query(sq_setup.results, str(sq_setup.filename), decode_raw=False)

        renamed_path = sq_setup.filename.with_stem(sq_setup.filename.stem + '_old')
        sq_mocks.rename.assert_called_once_with(renamed_path)
        sq_mocks.json.assert_called_once_with(sq_setup.results, sq_mocks.open(), indent=2)

    def test_save_query_permission_error(self, query_interactions, sq_setup, sq_mocks):
        """Test handling of PermissionError when renaming an existing file."""
        sq_mocks.exists.return_value = True
        sq_mocks.rename.side_effect = PermissionError("Cannot rename file")

        with pytest.raises(PermissionError):
            query_interactions.save_query(sq_setup.results, str(sq_setup.filename), decode_raw=False)

        query_interactions.log.critical.assert_called_once_with(
            'Failed to rename old file! Check that it is not in use or otherwise locked!'
        )

    def test_save_query_raw_data(self, query_interactions, sq_setup, sq_mocks, tmp_path):
        """Test saving when decode_raw is True, with raw data decoding and parquet file saving."""
        raw_value = base64.b64encode(b"fake_binary_data").decode("utf-8")
        sq_setup.results['events'][0]['RAWDATA_field'] = raw_value
        sq_setup.results['events'][0]['ORIG_FILE'] = 'source.json'

        query_interactions.save_query(sq_setup.results, str(sq_setup.filename), decode_raw=True)

        sq_mocks.json.assert_called_once_with(sq_setup.results, sq_mocks.open(), indent=2)

        raw_bytes = base64.b64decode(raw_value)
        parq_dir = "source"
        parq_name = "field"
        parquet_path = tmp_path / "rawdata" / parq_dir / f"{parq_name}.parquet"

        sq_mocks.mkdir.assert_any_call(parents=True, exist_ok=True)
        sq_mocks.open.assert_any_call(parquet_path, "wb")
        sq_mocks.open().write.assert_called_once_with(raw_bytes)