import sys
from abc import ABC, abstractmethod

from datawave_cli.utilities import pods


class BaseInteractions(ABC):
    def __init__(self, args):
        self.namespace = args.namespace
        self.init_base_url(args)
        self.init_cert(args)
        self.init_headers(args)

    def init_cert(self, args):
        if args.key is None:
            self.cert = args.cert
        else:
            self.cert = (args.cert, args.key)

    def init_base_url(self, args):
        """Define the base_url for interacting with datawave based on arguments
        passed in at call. Has the possibilities of localhost:port, IP:port, or an actual
        url.
        """
        if (args.localhost):
            self.base_url = "https://localhost:8443"
        elif (args.ip):
            pod_ip = self.get_pod_ip()
            self.base_url = f"https://{pod_ip}:8443"
        else:
            url = args.url
            if not url:
                self.log.critical("URL is none, cannot continue.")
                sys.exit(1)
            self.log.debug(url)
            self.base_url = f"https://{url}"
        self.log.debug(f"Base URL: {self.base_url}")

    def init_headers(self, args):
        self.headers = {k: v for k, v in args.header}
        self.log.debug(f"Headers Passed in: {self.headers}")

    def get_pod_ip(self):
        """Gets the pod IP for the selected pod."""
        return pods.get_specific_pod(self.pod_info, self.namespace).pod_ip

    @property
    @abstractmethod
    def pod_info(self):
        """Abstract attribute that subclasses must define to specify the pod they use."""
        pass