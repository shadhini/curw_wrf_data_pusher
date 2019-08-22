#!/usr/bin/env bash

# Print execution date time
echo `date`

echo "Changing into ~/wrf_data_pusher"
cd /home/uwcc-admin/curw_wrf_data_pusher
echo "Inside `pwd`"


# If no venv (python3 virtual environment) exists, then create one.
if [ ! -d "venv" ]
then
    echo "Creating venv python3 virtual environment."
    virtualenv -p python3 venv
fi

# Activate venv.
echo "Activating venv python3 virtual environment."
source venv/bin/activate

# Install dependencies using pip.
if [ ! -f "wrf_data_pusher.log" ]
then
    echo "Installing numpy"
    pip install numpy
    echo "Installing netCDF4"
    pip install netCDF4
    echo "Installing cftime"
    pip install cftime
    echo "Installing PyMySQL"
    pip install PyMySQL
    echo "Installing PyYAML"
    pip install PyYAML
    echo "Installing paramiko"
    pip install paramiko
    echo "Installing datalayer"
#    pip install git+https://github.com/shadhini/curw_db_adapter.git -U
    pip install git+https://github.com/shadhini/curw_db_adapter.git
fi

# Push WRFv4 data into the database
echo "Running scripts to push wrf data. Logs Available in wrf_data_pusher.log file."
python wrf_data_pusher.py >> wrf_data_pusher.log 2>&1

# Deactivating virtual environment
echo "Deactivating virtual environment"
deactivate

