
#!/bin/bash
sudo apt-get update && ACCEPT_EULA=Y apt-get install "/dbfs/FileStore/Libraries/msodbcsql17_17_10_1_1_1_amd64.deb" -y
pip install '/dbfs/FileStore/Libraries/pyodbc-4.0.34-cp38-cp38-manylinux_2_24_x86_64.whl' --force-reinstall