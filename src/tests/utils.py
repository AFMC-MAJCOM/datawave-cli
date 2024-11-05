from requests.exceptions import HTTPError, JSONDecodeError, Timeout


def create_mock_requests_get(mocker, mock_exception, mock_response_status, mock_response_text):
    mock_requests_get = mocker.patch('requests.get')

    mock_response = mocker.Mock()
    mock_response.status_code = mock_response_status
    mock_response.text = mock_response_text

    # add a side effect for an Exception
    match mock_exception:
        case JSONDecodeError():
            mock_response.json.side_effect = mock_exception
        case HTTPError():
            mock_response.raise_for_status.side_effect = mock_exception
        case _:
            mock_requests_get.side_effect = mock_exception

    mock_requests_get.return_value = mock_response

    return mock_requests_get, mock_response