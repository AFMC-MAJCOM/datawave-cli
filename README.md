# datawave-query-CLI

This repo contains python scripts simplifying interaction with datawave from CLI.

## Installation
The pipeline automatically builds and uploads the `.whl` file to frostbite-pypi when a push to the main branch is detected. However, during development, you'll need to build and install it yourself. See the [development section](#development) for details.

Since the wheel should be in the Frostbite PyPi, you will be able to just use `pip install datawave_cli` as long as you have your pip config set up to pull from there.

## Usage
At a baseline the tool is accessed through the command `datawave`. There are 5 subcommands that are used to access different aspects of Datawave. For each of these we have tried to make the `--help` option as self explanatory as possible.

The following is a list and quick description of each subcommand.
* `datawave accumulo`: Refreshes or views the Accumulo cache.
* `datawave authorization`: Returns the "whoami" information for the provided certificate.
* `datawave dictionary`: Displays the columns for the datatypes in the database.
* `datawave ingest`: Interacts with the ingest portion of Datawave. It ingests a file if one is provided; otherwise, it displays current ingest jobs.
* `datawave query`: Simplifies the query process with Datawave.

### Configuring Host and IP
By default, all commands target the hostname ``. This can be overridden in two ways:
1) Use the `-u` or `--url` option to specify a different hostname for that specific run.
1) Set the environment variable `DWV_URL` to always use a custom hostname. Unsetting this reverts to the default.

If you prefer to use an IP address (e.g., if DNS is unavailable), you can use the `-i` or `--ip` flag. This tells the tool to locate the IP based on Kubernetes pod information for the relevant service. In this case, you may also need to specify the namespace using the `-n` or `--namespace` option. If `-n` is omitted, the tool will attempt to retrieve the namespace from the environment variable `DWV_NAMESPACE`. If neither the option nor the environment variable is provided, the namespace defaults to `default`.

**Note:** When using the `--ip` option, ensure that your Kubernetes configuration (`kubeconfig`) is properly set up and points to the correct cluster. Without a valid and properly configured `kubeconfig`, the tool will be unable to retrieve the necessary pod information for resolving the IP address.


## Development
Unfortunately, building directly in WSL or using the `-e` option for local pip installation has issues. Instead, you'll need to use PowerShell and run the following command, `py -m build`, to build.

This will generate the `dist/*.whl` file that you can then use for pip installation. To simplify the process, you can utilize the provided install.sh script. Running this script will automatically uninstall the old version and reinstall the new one. This does not handle the build so you will have to do that yourself first.

You will need to manually increment the version in `pyproject.toml` and the `install.sh` when changes are made. Please update this once per story, not with every push.