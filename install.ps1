Remove-Item -Recurse dist
py -m build

pip uninstall datawave_cli -y

$PACKAGE = Resolve-Path "dist/datawave_cli*.whl" | Select-Object -ExpandProperty Path
pip install $PACKAGE
