from kubernetes import client, config
from kubernetes.client.api import core_v1_api
from kubernetes.client.rest import ApiException
from kubernetes.stream import stream

config.load_kube_config()
core_v1 = core_v1_api.CoreV1Api()


class PodInformation():
    """
    Information and functions to interact with a specific pod.

    Store information about specific pods. Contains helper functions to assist
    with specific pod interactions. Utilizes pod labels and namespace to
    perform a pod lookup to grab the full name of the pod of interest. Can be
    used to get a list of logs from the pod or execute a command within the pod.

    Parameters
    ----------
    pod_labels: list
        list of strings of labels to utilize when looking up the pod.
    log_dir: str
        path within the pod to find the logs of the running app.
    namespace: str
        the namespace to perform any kubernetes actions in.
        default: dev-datawave
    """
    def __init__(self, pod_labels: list, log_dir: str, namespace: str = 'dev-datawave'):
        self.pod_labels = pod_labels
        self.log_dir = log_dir
        self.namespace = namespace
        self.get_pod_name()

    def get_pod_name(self):
        """
        Utilizing the namespace and labels specified, search for the correct
        pod and grab the name of the first pod from the list.

        Raises
        ------
        RuntimeError
            Raised if no pod was found containing the specified labels within
            the namespace.
        """
        try:
            pods = core_v1.list_namespaced_pod(namespace=self.namespace,
                                               label_selector=','.join(self.pod_labels)).items
            self.podname = pods[0].metadata.name
            self.pod_ip = pods[0].status.pod_ip
        except ApiException as e:
            if e.status != 404:
                print(f"Unknown error: {e}")
                exit(1)
        except IndexError as e:
            raise RuntimeError(f"No pod found with the labels {self.pod_labels} in namespace {self.namespace}")

    def get_logs_files(self):
        """
        Obtains a list of the files specified at the log directory.
        """
    # View logs in a pod
        exec_command = f"ls {self.log_dir}"
        log_files = self.execute_cmd(exec_command)
        return log_files

    def execute_cmd(self, cmd_to_run: str):
        """
        Executes a command within the pod.

        Parameters
        ----------
        cmd_to_run: str
            the command you want to run within the pod

        Returns
        -------
        resp
            A string representing the response output of the executed command.

        Note
        ----
        With the stream function, When calling a pod with multiple containers
        running, the target container has to be specified with a keyword
        argument container=<name>.
        """
        cmd = [
            '/bin/sh',
            '-c',
            cmd_to_run
        ]

        resp = stream(core_v1.connect_get_namespaced_pod_exec,
                      self.podname,
                      self.namespace,
                      command=cmd,
                      stderr=True, stdin=False,
                      stdout=True, tty=False)
        return resp