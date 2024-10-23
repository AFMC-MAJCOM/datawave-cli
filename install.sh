pip uninstall datawave_cli -y

WHL_FILE=$(ls dist/datawave_cli-*.whl | sort -V | tail -n 1)
pip install "$WHL_FILE"