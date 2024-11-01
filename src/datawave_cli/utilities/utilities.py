import json
import time
import math
import logging

import requests
from pathlib import Path
from typing import Optional
from logging import Logger


class Retry:
    """Decorator to retry a function `tries` times or for `time_limit_min` minutes.

    Attempts to recall a function `tries` times or until the total elapsed
    time exceeds `time_limit_min` minutes. Will pause `delay_sec` seconds between
    each call and will return the last error raised as well as a `TimeoutError`
    if it exceeds maximum number of tries or goes over the time limit.

    Parameters
    ----------
    tries: int, optional
        Number of times to retry. Defaults to math.inf.

    time_limit_min: int, optional
        If not None, how many total minutes to spend retrying. Defaults to 10
        minutes.

    delay_sec: int, optional
        How many seconds to wait in between each attampt. Defaults to 5 seconds.

    Raises
    ------
    TimeoutError
        If `time_limit_min` is exceded or `tries` is reached. Also includes error
        raised by wrapped function.

    Notes
    -----
    With parameters of `tries=math.inf` and `time_limit_min=None` the function
    will be repeatedly called until it stops raising errors.
    """
    testing = False

    def __init__(self, f=None, tries=math.inf, time_limit_min=10, delay_sec=5):
        self.f = f
        self.tries = tries
        self.time_limit = time_limit_min * 60 if time_limit_min else None
        self.delay_sec = delay_sec

    def __call__(self, *args, **kwargs):
        if self.f is None:
            self.f = args[0]
        else:
            attempts = 0
            start_time = time.time()
            while True:
                try:
                    return self.f(*args, **kwargs)
                except Exception as e:
                    if Retry.testing:
                        raise e
                    attempts += 1
                    if attempts >= self.tries:
                        raise TimeoutError(f"Total number of retries exceeded {self.tries}", e)
                    if self.time_limit and time.time() - start_time >= self.time_limit:
                        raise TimeoutError(f"Total elapsed time exceeded {self.time_limit / 60:.1f} minutes", e)
                    if self.delay_sec:
                        time.sleep(self.delay_sec)
        return self


def setup_logger(name, log_file: Optional[str] = None, log_level: int | str = logging.INFO) -> Logger:
    """Setup a logger with specified name and optionally log to a file.

    Parameters
    ----------
    name: str
        The name of the logger.

    log_file: str, optional
        The path to the log file.

    log_level: int or str, optional
        Level to set logger. Defaults to logging.INFO

    Returns
    -------
    Logger
        The configured logger object

    Notes
    -----
    If `log_file` is not provided it will only output to terminal
    """
    logger = logging.getLogger(name)
    logger.setLevel(log_level)

    formatter = logging.Formatter('%(asctime)s : %(levelname)s - %(message)s')

    ch = logging.StreamHandler()
    ch.setLevel(log_level)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    if log_file is not None:
        log_path = Path(__file__).parent.joinpath(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(log_path)
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    return logger


def log_http_response(resp: requests.models.Response, log):
    log.debug(f"Response Status: {resp.status_code}, {resp.reason}")
    if resp.status_code != 200 and resp.status_code != 204:
        try:
            log.debug(json.dumps(resp.json(), indent=1))
        except requests.JSONDecodeError:
            log.debug(resp.text)


if __name__ == '__main__':
    pass
