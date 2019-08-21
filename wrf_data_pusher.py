import traceback
from netCDF4 import Dataset
import numpy as np
import os
import json
from datetime import datetime, timedelta
import time
import paramiko
import multiprocessing as mp
import sys

from db_adapter.base import get_Pool, destroy_Pool

from db_adapter.curw_fcst.source import get_source_id, add_source
from db_adapter.curw_fcst.variable import get_variable_id, add_variable
from db_adapter.curw_fcst.unit import get_unit_id, add_unit, UnitType
from db_adapter.curw_fcst.station import StationEnum, get_station_id, add_station, get_wrf_stations
from db_adapter.curw_fcst.timeseries import Timeseries
from db_adapter.constants import COMMON_DATE_TIME_FORMAT
from db_adapter.constants import (
    CURW_FCST_DATABASE, CURW_FCST_PASSWORD, CURW_FCST_USERNAME, CURW_FCST_PORT,
    CURW_FCST_HOST,
    )

from db_adapter.logger import logger

SRI_LANKA_EXTENT = [79.5213, 5.91948, 81.879, 9.83506]

wrf_v3_stations = { }

email_content = {}


def read_attribute_from_config_file(attribute, config):
    """
    :param attribute: key name of the config json file
    :param config: loaded json file
    :return:
    """
    if attribute in config and (config[attribute]!=""):
        return config[attribute]
    else:
        msg = "{} not specified in config file.".format(attribute)
        logger.error(msg)
        email_content[datetime.now().strftime(COMMON_DATE_TIME_FORMAT)] = msg
        sys.exit(1)


def get_per_time_slot_values(prcp):
    per_interval_prcp = (prcp[1:] - prcp[:-1])
    return per_interval_prcp


def get_file_last_modified_time(file_path):
    # returns local time (UTC + 5 30)
    modified_time = time.gmtime(os.path.getmtime(file_path) + 19800)

    return time.strftime('%Y-%m-%d %H:%M:%S', modified_time)


def datetime_utc_to_lk(timestamp_utc, shift_mins=0):
    return timestamp_utc + timedelta(hours=5, minutes=30 + shift_mins)


def ssh_command(ssh, command):
    ssh.invoke_shell()
    stdin, stdout, stderr = ssh.exec_command(command)
    if stdout.channel.recv_exit_status() is not 0:
        return False
    return True
    # for line in stdout.readlines():
    #     logger.info(line)
    # for line in stderr.readlines():
    #     logger.error(line)


def run_remote_command(host, user, key, command):
    """
    :return:  True if successful, False otherwise
    """
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname=host, username=user, key_filename=key)

        return ssh_command(ssh, command)
    except Exception as e:
        msg = "Connection failed :: {} :: {}".format(host, command.split('2>&1')[0])
        logger.error(msg)
        email_content[datetime.now().strftime(COMMON_DATE_TIME_FORMAT)]=msg
        return False
    finally:
        ssh.close()


def gen_kelani_basin_rfields(source_names, version, sim_tag, rfield_host, rfield_key, rfield_user):
    """
    Generate kelani basin rfields
    :param source_names: e.g.: WRF_A,WRF_C
    :param version: e.g.: v4.0
    :param rfield_host:
    :param sim_tag: e.g.: "evening_18hrs"
    :param rfield_key:
    :param rfield_user:
    :return: True if successful, False otherwise
    """
    rfield_command_kelani_basin = "nohup ./rfield_extractor/gen_kelani_basin_rfield.py -m {} -v {} -s {} " \
                                  "2>&1 ./rfield_extractor/rfield.log".format(source_names, version, sim_tag)

    logger.info("Generate {} kelani basin rfield files.".format(source_names))
    return run_remote_command(host=rfield_host, key=rfield_key, user=rfield_user,
                     command=rfield_command_kelani_basin)


def gen_all_d03_rfields(source_names, version, sim_tag, rfield_host, rfield_key, rfield_user):
    """
       Generate d03 rfields for SL extent
       :param source_names: e.g.: WRF_A,WRF_C
       :param version: e.g.: v4.0
       :param sim_tag: e.g.: "evening_18hrs"
       :param rfield_host:
       :param rfield_key:
       :param rfield_user:
       :return:  True if successful, False otherwise
    """
    rfield_command_d03 = "nohup  ./rfield_extractor/gen_SL_d03_rfield.py -m {} -v {} -s {} 2>&1 " \
                         "./rfield_extractor/rfield.log".format(source_names, version, sim_tag)

    logger.info("Generate {} d03 rfield files.".format(source_names))
    return run_remote_command(host=rfield_host, key=rfield_key, user=rfield_user,
                     command=rfield_command_d03)


def push_rainfall_to_db(ts, ts_data, tms_id, fgt):
    """

    :param ts: timeseries class instance
    :param ts_data: timeseries
    :return:
    """

    try:
        ts.insert_formatted_data(ts_data, True)  # upsert True
        ts.update_latest_fgt(id_=tms_id, fgt=fgt)
    except Exception:
        msg = "Inserting the timseseries for tms_id {} and fgt {} failed.".format(ts_data[0][0], ts_data[0][2])
        logger.error(msg)
        traceback.print_exc()
        email_content[datetime.now().strftime(COMMON_DATE_TIME_FORMAT)] = msg


def read_netcdf_file(pool, rainnc_net_cdf_file_path, tms_meta):
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
        msg = 'no rainnc netcdf :: {}'.format(rainnc_net_cdf_file_path)
        logger.warning(msg)
        email_content[datetime.now().strftime(COMMON_DATE_TIME_FORMAT)] = msg
        sys.exit(msg)
    else:

        try:
            """
            RAINNC netcdf data extraction
    
            """
            fgt = get_file_last_modified_time(rainnc_net_cdf_file_path)

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

                        run_meta = {
                            'tms_id': tms_id,
                            'sim_tag': tms_meta['sim_tag'],
                            'start_date': start_date,
                            'end_date': end_date,
                            'station_id': station_id,
                            'source_id': tms_meta['source_id'],
                            'unit_id': tms_meta['unit_id'],
                            'variable_id': tms_meta['variable_id']
                        }
                        try:
                            ts.insert_run(run_meta)
                        except Exception:
                            logger.error("Exception occurred while inserting run entry {}".format(run_meta))
                            traceback.print_exc()

                    data_list = []
                    # generate timeseries for each station
                    for i in range(len(diff)):
                        ts_time = datetime.strptime(time_unit_info_list[2], '%Y-%m-%dT%H:%M:%S') + timedelta(
                            minutes=times[i + 1].item())
                        t = datetime_utc_to_lk(ts_time, shift_mins=0)
                        data_list.append([tms_id, t.strftime('%Y-%m-%d %H:%M:%S'), fgt, float(diff[i, y, x])])

                    push_rainfall_to_db(ts=ts, ts_data=data_list, tms_id=tms_id, fgt=fgt)
            return True
        except Exception as e:
            msg = "netcdf file at {} reading error.".format(rainnc_net_cdf_file_path)
            logger.error(msg)
            traceback.print_exc()
            email_content[datetime.now().strftime(COMMON_DATE_TIME_FORMAT)] = msg
            sys.exit(msg)


def extract_wrf_data(wrf_system, config_data, tms_meta):
    logger.info(
        "######################################## {} #######################################".format(wrf_system))
    for date in config_data['dates']:

        #     /wrf_nfs/wrf/4.0/18/A/2019-07-30/d03_RAINNC.nc

        output_dir = os.path.join(config_data['wrf_dir'], config_data['version'], config_data['gfs_data_hour'],
                                  wrf_system, date)
        rainnc_net_cdf_file = 'd03_RAINNC.nc'

        rainnc_net_cdf_file_path = os.path.join(output_dir, rainnc_net_cdf_file)

        try:
            source_name = "{}_{}".format(config_data['model'], wrf_system)
            source_id = get_source_id(pool=pool, model=source_name, version=tms_meta['version'])

            if source_id is None:
                add_source(pool=pool, model=source_name, version=tms_meta['version'])
                source_id = get_source_id(pool=pool, model=source_name, version=tms_meta['version'])

        except Exception:
            msg = "Exception occurred while loading source meta data for WRF_{} from database.".format(wrf_system)
            logger.error(msg)
            email_content[datetime.now().strftime(COMMON_DATE_TIME_FORMAT)] = msg
            sys.exit(msg)

        tms_meta['model'] = source_name
        tms_meta['source_id'] = source_id

        read_netcdf_file(pool=pool, rainnc_net_cdf_file_path=rainnc_net_cdf_file_path, tms_meta=tms_meta)


if __name__ == "__main__":

    """
    Config.json 
    {
      "wrf_dir": "/wrf_nfs/wrf",
      "gfs_data_hour": "18",

      "version": "4.0",
      "model": "WRF",
      "wrf_systems": "A,C,E,SE",

      "run_date": ["2019-04-18","2019-04-17"],

      "sim_tag": "evening_18hrs",

      "unit": "mm",
      "unit_type": "Accumulative",
      "variable": "Precipitation",

      "rfield_host": "104.198.0.87",
      "rfield_user": "uwcc-admin",
      "rfield_key": "/home/uwcc-admin/.ssh/uwcc-admin"
    }

    /wrf_nfs/wrf/4.0/18/A/2019-07-30/d03_RAINNC.nc

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
        gfs_data_hour = read_attribute_from_config_file('gfs_data_hour', config)
        wrf_systems = read_attribute_from_config_file('wrf_systems', config)
        wrf_systems_list = wrf_systems.split(',')

        # sim_tag
        sim_tag = read_attribute_from_config_file('sim_tag', config)

        # unit details
        unit = read_attribute_from_config_file('unit', config)
        unit_type = UnitType.getType(read_attribute_from_config_file('unit_type', config))

        # variable details
        variable = read_attribute_from_config_file('variable', config)

        # rfield params
        rfield_host = read_attribute_from_config_file('rfield_host', config)
        rfield_user = read_attribute_from_config_file('rfield_user', config)
        rfield_key = read_attribute_from_config_file('rfield_key', config)

        dates = []

        if 'run_date' in config and (config['run_date'] != ""):
            dates = config['run_date']
        else:
            dates.append((datetime.now() + timedelta(hours=5, minutes=30)).strftime('%Y-%m-%d'))

        pool = get_Pool(host=CURW_FCST_HOST, port=CURW_FCST_PORT, user=CURW_FCST_USERNAME, password=CURW_FCST_PASSWORD,
                        db=CURW_FCST_DATABASE)

        try:
            wrf_v3_stations = get_wrf_stations(pool)

            variable_id = get_variable_id(pool=pool, variable=variable)
            unit_id = get_unit_id(pool=pool, unit=unit, unit_type=unit_type)
        except Exception:
            msg = "Exception occurred while loading common metadata from database."
            logger.error(msg)
            email_content[datetime.now().strftime(COMMON_DATE_TIME_FORMAT)] = msg
            sys.exit(1)

        tms_meta = {
            'sim_tag': sim_tag,
            'version': version,
            'variable': variable,
            'unit': unit,
            'unit_type': unit_type.value,
            'variable_id': variable_id,
            'unit_id': unit_id
        }

        config_data = {
            'model': model,
            'version': version,
            'dates': dates,
            'wrf_dir': wrf_dir,
            'gfs_data_hour': gfs_data_hour
        }

        mp_pool = mp.Pool(mp.cpu_count())

        # wrf_results = mp_pool.starmap_async(extract_wrf_data,
        #                                 [(wrf_system, config_data, tms_meta) for wrf_system in wrf_systems_list]).get()

        wrf_results = mp_pool.starmap(extract_wrf_data,
                                            [(wrf_system, config_data, tms_meta) for wrf_system in
                                             wrf_systems_list])

        print("wrf extraction results: ", wrf_results)

        source_list = ""

        for wrf_system in wrf_systems_list:
            source_list += "WRF_{},".format(wrf_system)

        source_list = source_list[:-1]

        kelani_basin_rfield_status = gen_kelani_basin_rfields(source_names=source_list, version=version, sim_tag=sim_tag,
                                                rfield_host=rfield_host, rfield_key=rfield_key, rfield_user=rfield_user)

        if not kelani_basin_rfield_status:
            email_content[datetime.now().strftime(COMMON_DATE_TIME_FORMAT)] = "Kelani basin rfiled generation for {} failed".format(source_list)

        d03_rfield_status = gen_all_d03_rfields(source_names=source_list, version=version, sim_tag=sim_tag,
                                                rfield_host=rfield_host, rfield_key=rfield_key, rfield_user=rfield_user)

        if not d03_rfield_status:
            email_content[datetime.now().strftime(COMMON_DATE_TIME_FORMAT)] = "SL d03 rfiled generation for {} failed".format(source_list)

    except Exception as e:
        msg = 'Multiprocessing error.'
        logger.error(msg)
        email_content[datetime.now().strftime(COMMON_DATE_TIME_FORMAT)] = msg
        traceback.print_exc()
    finally:
        mp_pool.close()
        destroy_Pool(pool)
        logger.info("Process finished.")
        logger.info("Email Content {}".format(json.dumps(email_content)))
