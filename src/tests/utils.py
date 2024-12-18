import json
from functools import wraps
from pathlib import Path

import pytest
import yaml
from requests.exceptions import HTTPError, JSONDecodeError


class ParamLoader:
    def __init__(self, file_path):
        full_path = Path(__file__).resolve().parent / file_path
        match full_path.suffix:
            case ".json" | ".yaml" | ".yml":
                pass
            case _:
                raise ValueError("Unsupported file format: use .json")
        if not full_path.exists():
            raise FileNotFoundError(f"{file_path} does not exist!")
        self.file_path = full_path

    def load_parameters(self, test_name):
        """Load test parameters for a given test name from the JSON or YAML file."""
        with open(self.file_path, 'r') as file:
            match self.file_path.suffix:
                case ".json":
                    all_parameters = json.load(file)
                case ".yaml" | ".yml":
                    all_parameters = yaml.safe_load(file)
        data = all_parameters.get(test_name, None)
        if not data or any([key not in data for key in ['argnames', 'args']]):
            raise RuntimeError(f"{test_name} is not properly formatted in {self.file_path}")
        return data['argnames'], data['args'], data.get('ids', [])

    def parametrize(self, func):
        """Decorator to automatically apply pytest.mark.parametrize with loaded parameters."""
        test_name = func.__name__
        argnames, args, ids = self.load_parameters(test_name)

        @wraps(func)
        @pytest.mark.parametrize(
            argnames, args, ids=ids
        )
        def wrapper(*func_args, **func_kwargs):
            return func(*func_args, **func_kwargs)
        return wrapper


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