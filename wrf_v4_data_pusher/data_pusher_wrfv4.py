import traceback
from netCDF4 import Dataset
import numpy as np
import os
import json
from datetime import datetime, timedelta
import time
from pymysql import IntegrityError

from db_adapter.base import get_Pool

from db_adapter.curw_fcst.source import get_source_id, add_source
from db_adapter.curw_fcst.variable import get_variable_id, add_variable
from db_adapter.curw_fcst.unit import get_unit_id, add_unit, UnitType
from db_adapter.curw_fcst.station import StationEnum, get_station_id, add_station, get_wrfv3_stations
from db_adapter.curw_fcst.timeseries import Timeseries
from db_adapter.exceptions import DuplicateEntryError

from logger import logger

SRI_LANKA_EXTENT = [79.5213, 5.91948, 81.879, 9.83506]

wrf_v3_stations = {}


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
        ts.insert_data(ts_data, True) # upsert True
    # except DuplicateEntryError:
    #     logger.info("Timseries id already exists in the database : {}".format(ts_run[0]))
    #     logger.info("For the meta data : {}".format(ts_run))
    #     pass
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
        print('no rainnc netcdf')
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
        print('[lon_min, lat_min, lon_max, lat_max] :', [lon_min, lat_min, lon_max, lat_max])

        lat_inds = np.where((lats >= lat_min) & (lats <= lat_max))
        lon_inds = np.where((lons >= lon_min) & (lons <= lon_max))

        rainnc = nnc_fid.variables['RAINNC'][:, lat_inds[0], lon_inds[0]]

        times = nnc_fid.variables['XTIME'][:]

        start_date = fgt
        end_date = fgt

        nnc_fid.close()

        width = len(lons)
        height = len(lats)

        ts = Timeseries(pool)

        for y in range(height):
            for x in range(width):

                lat = float(lats[y])
                lon = float(lons[x])

                tms_meta['latitude'] = str(lat)
                tms_meta['longitude'] = str(lon)

                station_prefix = '{}_{}'.format(lat, lon)

                station_id = wrf_v3_stations.get(station_prefix)

                if station_id is None:
                    add_station(pool=pool, name=station_prefix, latitude=lat, longitude=lon,
                            description="WRF point", station_type=StationEnum.WRF)

                tms_id = ts.get_timeseries_id_if_exists(tms_meta)
                logger.info("Existing timeseries id: {}".format(tms_id))

                if tms_id is None:
                    tms_id = ts.generate_timeseries_id(tms_meta)
                    logger.info('HASH SHA256 created: {}'.format(tms_id))

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
                for i in range(len(rainnc)):
                    ts_time = datetime.strptime(time_unit_info_list[2], '%Y-%m-%dT%H:%M:%S') + timedelta(
                            minutes=times[i].item())
                    t = datetime_utc_to_lk(ts_time, shift_mins=0)
                    data_list.append([tms_id, t.strftime('%Y-%m-%d %H:%M:%S'), fgt, float(rainnc[i, y, x])])

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
        if 'wrf_dir' in config and (config['wrf_dir']!=""):
            wrf_dir = config['wrf_dir']
        else:
            logger.error("wrf_dir not specified in config file.")
            exit(1)

        if 'model' in config and (config['model']!=""):
            model = config['model']
        else:
            logger.error("model not specified in config file.")
            exit(1)

        if 'version' in config and (config['version']!=""):
            version = config['version']
        else:
            logger.error("version not specified in config file.")
            exit(1)

        if 'wrf_model_list' in config and (config['wrf_model_list']!=""):
            wrf_model_list = config['wrf_model_list']
            wrf_model_list = wrf_model_list.split(',')
        else:
            logger.error("wrf_model_list not specified in config file.")
            exit(1)

        # unit details
        if 'unit' in config and (config['unit']!=""):
            unit = config['unit']
        else:
            logger.error("unit not specified in config file.")
            exit(1)

        if 'unit_type' in config and (config['unit_type']!=""):
            unit_type = UnitType.getType(config['unit_type'])
        else:
            logger.error("unit_type not specified in config file.")
            exit(1)

        # variable details
        if 'variable' in config and (config['variable']!=""):
            variable = config['variable']
        else:
            logger.error("variable not specified in config file.")
            exit(1)

        # connection params
        if 'host' in config and (config['host']!=""):
            host = config['host']
        else:
            logger.error("host not specified in config file.")
            exit(1)

        if 'user' in config and (config['user']!=""):
            user = config['user']
        else:
            logger.error("user not specified in config file.")
            exit(1)

        if 'password' in config and (config['password']!=""):
            password = config['password']
        else:
            logger.error("password not specified in config file.")
            exit(1)

        if 'db' in config and (config['db']!=""):
            db = config['db']
        else:
            logger.error("db not specified in config file.")
            exit(1)

        if 'port' in config and (config['port']!=""):
            port = config['port']
        else:
            logger.error("port not specified in config file.")
            exit(1)

        if 'start_date' in config and (config['start_date']!=""):
            run_date_str = config['start_date']
            # fgt = (datetime.strptime(run_date_str, '%Y-%m-%d') + timedelta(days=1)) \
            #     .strftime('%Y-%m-%d 23:45:00')
        else:
            run_date_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            # fgt = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')

        daily_dir = 'STATIONS_{}'.format(run_date_str)

        output_dir = os.path.join(wrf_dir, daily_dir)

        pool = get_Pool(host=host, port=port, user=user, password=password, db=db)

        wrf_v3_stations = get_wrfv3_stations(pool)

        # # Retrieve db version.
        # conn = pool.get_conn()
        # with conn.cursor() as cursor:
        #     cursor.execute("SELECT VERSION()")
        #     data = cursor.fetchone()
        #     logger.info("Database version : %s " % data)
        # if conn is not None:
        #     pool.release(conn)

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
                print('Net CDF file reading error.')
                traceback.print_exc()

        # try:
        #     fgt = datetime_utc_to_lk(datetime.now(), shift_mins=0).strftime('%Y-%m-%d %H:%M:%S')
        #     ts.update_fgt(scheduled_date=scheduled_date, fgt=fgt)
        # except Exception as e:
        #         logger.error('Exception occurred while updating fgt')
        #         print('Exception occurred while updating fgt')
        #         traceback.print_exc()

        pool.destroy()

    except Exception as e:
        logger.error('JSON config data loading error.')
        print('JSON config data loading error.')
        traceback.print_exc()
    finally:
        logger.info("Process finished.")
        print("Process finished.")
