#!/usr/bin/env bash

echo `date` 

echo "Changing into ~//wrf_v3_data_pusher/create_schema"
cd /home/uwcc-admin/wrf_v3_data_pusher/create_schema
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
echo "Installing db_adapter"
pip install git+https://github.com/shadhini/curw_db_adapter.git -U


# Create schema
echo "Running create_schema.py"
python create_schema.py

# Deactivating virtual environment
echo "Deactivating virtual environment"
deactivate
