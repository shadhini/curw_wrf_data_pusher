import traceback
from netCDF4 import Dataset
import numpy as np
import os
import json
from datetime import datetime, timedelta
import time
import paramiko

from db_adapter.base import get_Pool, destroy_Pool

from db_adapter.curw_fcst.source import get_source_id, add_source
from db_adapter.curw_fcst.variable import get_variable_id, add_variable
from db_adapter.curw_fcst.unit import get_unit_id, add_unit, UnitType
from db_adapter.curw_fcst.station import StationEnum, get_station_id, add_station, get_wrf_stations
from db_adapter.curw_fcst.timeseries import Timeseries

from logger import logger

SRI_LANKA_EXTENT = [79.5213, 5.91948, 81.879, 9.83506]

wrf_v3_stations = {}


def read_attribute_from_config_file(attribute, config):
    """
    :param attribute: key name of the config json file
    :param config: loaded json file
    :return:
    """
    if attribute in config and (config[attribute]!=""):
        return config[attribute]
    else:
        logger.error("{} not specified in config file.".format(attribute))
        exit(1)


def ssh_command(ssh, command):
    ssh.invoke_shell()
    stdin, stdout, stderr = ssh.exec_command(command)
    for line in stdout.readlines():
        print(line)
    for line in stderr.readlines():
        print(line)


def gen_rfield_files(host, user, key, command):
    try:
        ssh = paramiko.SSHClient()
        print('Calling paramiko')
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname=host, username=user, key_filename=key)

        ssh_command(ssh, command)
    except Exception as e:
        print('Connection Failed')
        print(e)
    finally:
        print("Close connection")
        ssh.close()


def get_per_time_slot_values(prcp):
    per_interval_prcp = (prcp[1:] - prcp[:-1])
    return per_interval_prcp


def get_file_last_modified_time(file_path):

    # returns local time (UTC + 5 30)
    modified_time = time.gmtime(os.path.getmtime(file_path) + 19800)

    return time.strftime('%Y-%m-%d %H:%M:%S', modified_time)


def push_rainfall_to_db(ts, ts_data):
    """

    :param ts: timeseries class instance
    :param ts_data: timeseries
    :return:
    """

    try:
        ts.insert_data(ts_data, True)  # upsert True
    except Exception:
        logger.error("Inserting the timseseries for tms_id {} and fgt {} failed.".format(ts_data[0][0], ts_data[0][2]))
        traceback.print_exc()
        return False


def datetime_utc_to_lk(timestamp_utc, shift_mins=0):
    return timestamp_utc + timedelta(hours=5, minutes=30 + shift_mins)


def read_netcdf_file(pool, rainnc_net_cdf_file_path,
                     source_id, variable_id, unit_id, tms_meta, fgt):
    """

    :param pool: database connection pool
    :param rainnc_net_cdf_file_path:
    :param source_id:
    :param variable_id:
    :param unit_id:
    :param tms_meta:
    :return:

    rainc_unit_info:  mm
    lat_unit_info:  degree_north
    time_unit_info:  minutes since 2019-04-02T18:00:00
    """

    if not os.path.exists(rainnc_net_cdf_file_path):
        logger.warning('no rainnc netcdf')
    else:

        """
        RAINNC netcdf data extraction
        """
        nnc_fid = Dataset(rainnc_net_cdf_file_path, mode='r')

        time_unit_info = nnc_fid.variables['XTIME'].units

        time_unit_info_list = time_unit_info.split(' ')

        lats = nnc_fid.variables['XLAT'][0, :, 0]
        lons = nnc_fid.variables['XLONG'][0, 0, :]

        lon_min = lons[0].item()
        lat_min = lats[0].item()
        lon_max = lons[-1].item()
        lat_max = lats[-1].item()

        lat_inds = np.where((lats >= lat_min) & (lats <= lat_max))
        lon_inds = np.where((lons >= lon_min) & (lons <= lon_max))

        rainnc = nnc_fid.variables['RAINNC'][:, lat_inds[0], lon_inds[0]]

        times = nnc_fid.variables['XTIME'][:]

        start_date = fgt
        end_date = fgt

        nnc_fid.close()

        diff = get_per_time_slot_values(rainnc)

        width = len(lons)
        height = len(lats)

        ts = Timeseries(pool)

        for y in range(height):
            for x in range(width):

                lat = float('%.6f' % lats[y])
                lon = float('%.6f' % lons[x])

                tms_meta['latitude'] = str(lat)
                tms_meta['longitude'] = str(lon)

                station_prefix = 'wrf_{}_{}'.format(lat, lon)

                station_id = wrf_v3_stations.get(station_prefix)

                if station_id is None:
                    add_station(pool=pool, name=station_prefix, latitude=lat, longitude=lon,
                            description="WRF point", station_type=StationEnum.WRF)
                    station_id = get_station_id(pool=pool, latitude=lat, longitude=lon, station_type=StationEnum.WRF)

                tms_id = ts.get_timeseries_id_if_exists(tms_meta)

                if tms_id is None:
                    tms_id = ts.generate_timeseries_id(tms_meta)

                    run = (tms_id, tms_meta['sim_tag'], start_date, end_date, station_id, source_id, variable_id, unit_id)
                    try:
                        ts.insert_run(run)
                    except Exception:
                        logger.error("Exception occurred while inserting run entry {}".format(run))
                        traceback.print_exc()
                else:
                    ts.update_latest_fgt(id_=tms_id, fgt=fgt)

                data_list = []
                # generate timeseries for each station
                for i in range(len(diff)):
                    ts_time = datetime.strptime(time_unit_info_list[2], '%Y-%m-%dT%H:%M:%S') + timedelta(
                            minutes=times[i+1].item())
                    t = datetime_utc_to_lk(ts_time, shift_mins=0)
                    data_list.append([tms_id, t.strftime('%Y-%m-%d %H:%M:%S'), fgt, float(diff[i, y, x])])

                push_rainfall_to_db(ts=ts, ts_data=data_list)


if __name__=="__main__":

    """
    Config.json 
    {
      "wrf_dir": "/mnt/disks/wrf-mod",
      "model": "WRF",
      "version": "v3",
      "wrf_model_list": "A,C,E,SE",

      "start_date": "2019-03-24",

      "host": "127.0.0.1",
      "user": "root",
      "password": "password",
      "db": "curw_fcst",
      "port": 3306,

      "unit": "mm",
      "unit_type": "Accumulative",

      "variable": "Precipitation"
    }

    run_date_str :  2019-03-23
    daily_dir :  STATIONS_2019-03-23
    output_dir :  /mnt/disks/wrf-mod/STATIONS_2019-03-23
    sim_tag :  WRFv3_A
    rainnc_net_cdf_file :  d03_RAINNC_2019-03-23_A.nc
    rainnc_net_cdf_file_path :  /mnt/disks/wrf-mod/STATIONS_2019-03-23/d03_RAINNC_2019-03-23_A.nc    

    tms_meta = {
                    'sim_tag'       : sim_tag,
                    'latitude'      : latitude,
                    'longitude'     : longitude,
                    'model'         : model,
                    'version'       : version,
                    'variable'      : variable,
                    'unit'          : unit,
                    'unit_type'     : unit_type
                    }
    """
    try:
        config = json.loads(open('config.json').read())

        # source details
        wrf_dir = read_attribute_from_config_file('wrf_dir', config)
        model = read_attribute_from_config_file('model', config)
        version = read_attribute_from_config_file('version', config)
        wrf_model_list = read_attribute_from_config_file('wrf_model_list', config)
        wrf_model_list = wrf_model_list.split(',')

        # unit details
        unit = read_attribute_from_config_file('unit', config)
        unit_type = UnitType.getType(read_attribute_from_config_file('unit_type', config))

        # variable details
        variable = read_attribute_from_config_file('variable', config)

        # connection params
        host = read_attribute_from_config_file('host', config)
        user = read_attribute_from_config_file('user', config)
        password = read_attribute_from_config_file('password', config)
        db = read_attribute_from_config_file('db', config)
        port = read_attribute_from_config_file('port', config)

        # rfield params
        rfield_host = read_attribute_from_config_file('rfield_host', config)
        rfield_user = read_attribute_from_config_file('rfield_user', config)
        rfield_key = read_attribute_from_config_file('rfield_key', config)
        rfield_command = read_attribute_from_config_file('rfield_command', config)

        if 'start_date' in config and (config['start_date']!=""):
            run_date_str = config['start_date']
        else:
            run_date_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

        daily_dir = 'STATIONS_{}'.format(run_date_str)

        output_dir = os.path.join(wrf_dir, daily_dir)

        pool = get_Pool(host=host, port=port, user=user, password=password, db=db)

        wrf_v3_stations = get_wrf_stations(pool)

        variable_id = get_variable_id(pool=pool, variable=variable)
        unit_id = get_unit_id(pool=pool, unit=unit, unit_type=unit_type)

        for wrf_model in wrf_model_list:
            rainnc_net_cdf_file = 'd03_RAINNC_{}_{}.nc'.format(run_date_str, wrf_model)

            rainnc_net_cdf_file_path = os.path.join(output_dir, rainnc_net_cdf_file)
            logger.info("rainnc_net_cdf_file_path : {}".format(rainnc_net_cdf_file_path))

            fgt = get_file_last_modified_time(rainnc_net_cdf_file_path)

            sim_tag = 'evening_18hrs'
            source_name = "{}_{}".format(model, wrf_model)
            source_id = get_source_id(pool=pool, model=source_name, version=version)

            tms_meta = {
                    'sim_tag'       : sim_tag,
                    'model'         : source_name,
                    'version'       : version,
                    'variable'      : variable,
                    'unit'          : unit,
                    'unit_type'     : unit_type.value
                    }

            try:
                read_netcdf_file(pool=pool, rainnc_net_cdf_file_path=rainnc_net_cdf_file_path,
                        source_id=source_id, variable_id=variable_id, unit_id=unit_id, tms_meta=tms_meta, fgt=fgt)
            except Exception as e:
                logger.error("Net CDF file reading error.")
                traceback.print_exc()

        destroy_Pool(pool)
    except Exception as e:
        logger.error('JSON config data loading error.')
        traceback.print_exc()
    finally:
        logger.info("Generate rfield files.")
        gen_rfield_files(host=rfield_host, key=rfield_key, user=rfield_user, command=rfield_command)
        logger.info("Process finished.")
