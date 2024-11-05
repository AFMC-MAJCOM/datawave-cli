import pytest
import json
from functools import wraps
from pathlib import Path


class ParamLoader:
    def __init__(self, file_path):
        full_path = Path(__file__).resolve().parent / file_path
        if full_path.suffix != '.json':
            raise ValueError("Unsupported file format: use .json")
        if not full_path.exists():
            raise FileNotFoundError(f"{file_path} does not exist!")
        self.file_path = full_path

    def load_parameters(self, test_name):
        """Load test parameters for a given test name from the JSON file."""
        with open(self.file_path, 'r') as file:
            all_parameters = json.load(file)
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
