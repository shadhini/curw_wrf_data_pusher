import traceback
from netCDF4 import Dataset
import numpy as np
import os
import json
from datetime import datetime, timedelta
import time
import paramiko
import multiprocessing as mp

from db_adapter.base import get_Pool, destroy_Pool

from db_adapter.curw_fcst.source import get_source_id, add_source
from db_adapter.curw_fcst.variable import get_variable_id, add_variable
from db_adapter.curw_fcst.unit import get_unit_id, add_unit, UnitType
from db_adapter.curw_fcst.station import StationEnum, get_station_id, add_station, get_wrf_stations
from db_adapter.curw_fcst.timeseries import Timeseries
from db_adapter.constants import (
    CURW_FCST_DATABASE, CURW_FCST_PASSWORD, CURW_FCST_USERNAME, CURW_FCST_PORT,
    CURW_FCST_HOST,
    )

from logger import logger

SRI_LANKA_EXTENT = [79.5213, 5.91948, 81.879, 9.83506]

wrf_v3_stations = { }


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


def gen_rfield_files(host, user, key, command, wrf_model):
    try:
        ssh = paramiko.SSHClient()
        logger.info("Calling paramiko :: WRF_{}".format(wrf_model))
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname=host, username=user, key_filename=key)

        ssh_command(ssh, command)
    except Exception as e:
        logger.error("Connection failed :: WRF_{}".format(wrf_model))
        print(e)
    finally:
        logger.info("Connection closed :: WRF_{}".format(wrf_model))
        ssh.close()


def get_per_time_slot_values(prcp):
    per_interval_prcp = (prcp[1:] - prcp[:-1])
    return per_interval_prcp


def get_file_last_modified_time(file_path):
    # returns local time (UTC + 5 30)
    modified_time = time.gmtime(os.path.getmtime(file_path) + 19800)

    return time.strftime('%Y-%m-%d %H:%M:%S', modified_time)


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
        logger.error("Inserting the timseseries for tms_id {} and fgt {} failed.".format(ts_data[0][0], ts_data[0][2]))
        traceback.print_exc()
        return False


def datetime_utc_to_lk(timestamp_utc, shift_mins=0):
    return timestamp_utc + timedelta(hours=5, minutes=30 + shift_mins)


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
        logger.warning('no rainnc netcdf :: {}'.format(rainnc_net_cdf_file_path))
        return
    else:

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
                            'tms_id'     : tms_id,
                            'sim_tag'    : tms_meta['sim_tag'],
                            'start_date' : start_date,
                            'end_date'   : end_date,
                            'station_id' : station_id,
                            'source_id'  : tms_meta['source_id'],
                            'unit_id'    : tms_meta['unit_id'],
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


def extract_wrf_data(wrf_model, config_data, tms_meta):
    logger.info("######################################## {} #######################################".format(wrf_model))
    for date in config_data['dates']:
        run_date_str = date
        daily_dir = 'STATIONS_{}'.format(run_date_str)

        output_dir = os.path.join(config_data['wrf_dir'], daily_dir)
        rainnc_net_cdf_file = 'd03_RAINNC_{}_{}.nc'.format(run_date_str, wrf_model)

        rainnc_net_cdf_file_path = os.path.join(output_dir, rainnc_net_cdf_file)

        source_name = "{}_{}".format(config_data['model'], wrf_model)
        source_id = get_source_id(pool=pool, model=source_name, version=config_data['version'])

        tms_meta['model'] = source_name
        tms_meta['source_id'] = source_id

        try:
            read_netcdf_file(pool=pool, rainnc_net_cdf_file_path=rainnc_net_cdf_file_path, tms_meta=tms_meta)
            # if date==dates[-1]:
            #     try:
            #         # "rfield_command1": "nohup python /home/uwcc-admin/rfield_extractor/gen_rfield_kelani_basin.py -m WRF_A -v v4 &> /home/uwcc-admin/rfield_extractor/nohup.out",
            #         rfield_command_kelani_basin = "nohup python /home/uwcc-admin/rfield_extractor/" \
            #                                       "gen_rfield_kelani_basin.py -m {} -v {} &> " \
            #                                       "/home/uwcc-admin/rfield_extractor/nohup.out".format(source_name,
            #                 version)
            #         rfield_command_d03 = "nohup python /home/uwcc-admin/rfield_extractor/" \
            #                              "gen_rfield_d03.py -m {} -v {} &> " \
            #                              "/home/uwcc-admin/rfield_extractor/nohup.out".format(source_name, version)
            #
            #         logger.info("Generate WRF_{} kelani basin rfield files.".format(wrf_model))
            #         gen_rfield_files(host=config_data['rfield_host'], key=config_data['rfield_key'], user=config_data['rfield_user'],
            #                 command=rfield_command_kelani_basin, wrf_model=wrf_model)
            #         logger.info("Generate WRF_{} d03 rfield files.".format(wrf_model))
            #         gen_rfield_files(host=config_data['rfield_host'], key=config_data['rfield_key'], user=config_data['rfield_user'],
            #                 command=rfield_command_d03, wrf_model=wrf_model)
            #     except Exception as e:
            #         logger.error("Exception occurred while generating rfields for WRF_{}.".format(wrf_model))

        except Exception as e:
            logger.error("WRF_{} netcdf file reading error.".format(wrf_model))
            traceback.print_exc()


if __name__=="__main__":

    """
    Config.json 
    {
      "wrf_dir": "/mnt/disks/wrf-mod",
      "model": "WRF",
      "version": "v3",
      "wrf_model_list": "A,C,E,SE",
    
      "start_date": ["2019-04-18","2019-04-17"],
    
      "sim_tag": "evening_18hrs",
    
      "unit": "mm",
      "unit_type": "Accumulative",
      "variable": "Precipitation",
      
      "rfield_host": "104.198.0.87",
      "rfield_user": "uwcc-admin",
      "rfield_key": "/home/uwcc-admin/.ssh/uwcc-admin"
    }


    run_date_str :  2019-03-23
    daily_dir :  STATIONS_2019-03-23
    output_dir :  /mnt/disks/wrf-mod/STATIONS_2019-03-23
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

        if 'start_date' in config and (config['start_date']!=""):
            dates = config['start_date']
        else:
            dates.append((datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'))

        pool = get_Pool(host=CURW_FCST_HOST, port=CURW_FCST_PORT, user=CURW_FCST_USERNAME, password=CURW_FCST_PASSWORD,
                db=CURW_FCST_DATABASE)

        wrf_v3_stations = get_wrf_stations(pool)

        variable_id = get_variable_id(pool=pool, variable=variable)
        unit_id = get_unit_id(pool=pool, unit=unit, unit_type=unit_type)

        tms_meta = {
                'sim_tag'    : sim_tag,
                'version'    : version,
                'variable'   : variable,
                'unit'       : unit,
                'unit_type'  : unit_type.value,
                'variable_id': variable_id,
                'unit_id'    : unit_id
                }

        config_data = {
                'model'      : model,
                'version'    : version,
                'dates'      : dates,
                'wrf_dir'    : wrf_dir,
                'rfield_host': rfield_host,
                'rfield_key' : rfield_key,
                'rfield_user': rfield_user
                }

        mp_pool = mp.Pool(mp.cpu_count())

        results = mp_pool.starmap_async(extract_wrf_data, [(wrf_model, config_data, tms_meta) for wrf_model in wrf_model_list]).get()

        print("results: ", results)

        mp_pool.close()

        destroy_Pool(pool)

    except Exception as e:
        logger.error('Config data or meta data loading error.')
        traceback.print_exc()
    finally:
        logger.info("Process finished.")
