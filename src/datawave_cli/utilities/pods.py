import os
from datawave_cli.utilities.pod_information import PodInformation

namespace = os.environ.get('DWV_NAMESPACE', 'default')

"""
List of pods where we have specific interactions
"""
ingest_info = {"labels": ['app.kubernetes.io/component=ingest'], "pod_logs": '/srv/logs/ingest'}
yarn_rm_info = {"labels": ['component=yarn-rm'], "pod_logs": ''}
hdfs_nn_info = {"labels": ['component=hdfs-nn'], "pod_logs": ''}
web_datawave_info = {"labels": ['application=datawave-monolith'], "pod_logs": ''}
web_dictionary_info = {"labels": ['application=dictionary'], "pod_logs": ''}
web_authorization_info = {"labels": ['application=authorization'], "pod_logs": ''}


def get_pod(labels: list, pod_logs: str, namespace: str):
    """
    Create a PodInformation object using the information passed in.

    Parameters
    ----------
    labels: list
        a list of the labels to use in the pod search.
    pod_logs: str
        location within the pods to find the log files.
    namespace
        the namespace to use when getting pod information.
    """
    return PodInformation(labels, pod_logs, namespace=namespace)


def get_specific_pod(pod_info: dict, namespace: str):
    """
    Using predefined info, get a specific pod.

    Parameters
    ----------
    pod_info: dict
        A dict consisting of labels and pod_logs to use to get the pod info.
    namespace: str
        the namespace to use when getting pod information.
    """
    return get_pod(pod_info['labels'], pod_info['pod_logs'], namespace)