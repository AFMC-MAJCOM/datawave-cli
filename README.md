# datawave-query-CLI

This repository contains Python scripts simplifying interaction with Datawave from the command line interface (CLI).

## Installation
During development, the tool needs to be built and installed manually. See the [development section](#development) for details.

Once the `.whl` file is available, it can be installed using `pip`.

## Usage
The tool is accessed through the command `datawave`. There are 5 subcommands that provide access to different aspects of Datawave. For each of these, the `--help` option has been designed to be as self-explanatory as possible.

The following is a list and brief description of each subcommand:
* `datawave accumulo`: Refreshes or views the Accumulo cache.
* `datawave authorization`: Returns the "whoami" information for the provided certificate.
* `datawave dictionary`: Displays the columns for the data types in the database.
* `datawave ingest`: Interacts with the ingest portion of Datawave. It ingests a file if one is provided; otherwise, it displays current ingest jobs.
* `datawave query`: Simplifies the query process with Datawave.

### Configuring Host, IP, and Localhost
By default, all commands will target an empty hostname and fail. This can be overridden in the following ways:

1) Set the environment variable `DWV_URL` to always use a custom hostname. Unsetting this reverts to the default behavior.
1) Use the `-u` or `--url` option to specify a different hostname for that specific run.
1) Use the `-i` or `--ip` flag to specify an IP address instead of a hostname. This instructs the tool to locate the IP based on Kubernetes pod information for the relevant service.
    - If necessary, specify the namespace using the `-n` or `--namespace` option. If `-n` is omitted, the tool will attempt to retrieve the namespace from the environment variable `DWV_NAMESPACE`. If neither the option nor the environment variable is provided, the namespace defaults to `default`.
1) Use the `-l` or `--localhost` option to target the local machine (localhost). This option overrides any URL or IP settings, making the tool connect to `localhost` (127.0.0.1).

**Notes:**
- When using the `--ip` option, ensure that the Kubernetes configuration (`kubeconfig`) is properly set up and points to the correct cluster. Without a valid and properly configured `kubeconfig`, the tool will be unable to retrieve the necessary pod information for resolving the IP address.
- The `-l` / `--localhost` option is ideal for local testing and will bypass Kubernetes or DNS resolution altogether.

## Development
#### PowerShell
A provided `install.ps1` script automates the build, uninstall, and reinstall process for the CLI. Running this script will:

1. Build the project using `py -m build`.
2. Uninstall any existing version of the CLI.
3. Reinstall the newly built version.

To run the script, open PowerShell and execute the following command: `./install.ps1`. This simplifies the process by handling all steps in one command, ensuring the CLI is up-to-date with the latest build.

#### WSL
Building directly in WSL or using the `-e` option for local pip installation has known issues. Therefore, it is recommended to use PowerShell for the build, uninstall, and reinstall process as described in the [PowerShell section](#powershell).

However, if building in WSL is necessary, use the following command from powershell to generate the `.whl` file: `py -m build`

This will create the `dist/*.whl` file, which can then be installed manually. Note that the `install.sh` script can automate the uninstall and reinstall steps but does not handle the build process.

#### Versioning
When changes are made, increment the version manually in both the `pyproject.toml` and `install.sh` scripts. Version updates should be done once per story, rather than with every individual push.
