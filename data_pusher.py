import traceback
from netCDF4 import Dataset
import numpy as np
import os
import json
from datetime import datetime, timedelta

from db_adapter.curw_fcst.source import get_source_id, add_source
from db_adapter.curw_fcst.variable import get_variable_id, add_variable
from db_adapter.curw_fcst.unit import get_unit_id, add_unit, UnitType
from db_adapter.curw_fcst.station import StationEnum, get_station_id, add_station
from db_adapter.base import get_engine, get_sessionmaker, base
from db_adapter.constants import DRIVER_PYMYSQL, DIALECT_MYSQL
from db_adapter.curw_fcst.timeseries import Timeseries

from logger import logger

SRI_LANKA_EXTENT = [79.5213, 5.91948, 81.879, 9.83506]


def push_rainfall_to_db(session, engine, ts_data, ts_run):
    """

    :param session:
    :param engine:
    :param ts_data: timeseries
    :param ts_run: run entry
    :return:
    """

    try:
        return ts.insert_timeseries(tms_id=tms_id, timeseries=timeseries, fgt=None,
                sim_tag=tms_meta["sim_tag"], scheduled_date=tms_meta["scheduled_date"],
                station_id=station_id, source_id=source_id, variable_id=variable_id, unit_id=unit_id,
                start_date=start_date, end_date=end_date)
    except Exception:
        logger.error("Exception occurred while inserting the timseseries for tms_id {}".format(tms_id))
        traceback.print_exc()
        return False
    finally:
        session.close()


def get_two_element_average(prcp, return_diff=True):
    avg_prcp = (prcp[1:] + prcp[:-1]) * 0.5
    if return_diff:
        return avg_prcp - np.insert(avg_prcp[:-1], 0, [0], axis=0)
    else:
        return avg_prcp


def datetime_utc_to_lk(timestamp_utc, shift_mins=0):
    return timestamp_utc + timedelta(hours=5, minutes=30 + shift_mins)


def read_netcdf_file(session, engine, rainc_net_cdf_file_path, rainnc_net_cdf_file_path,
                     source_id, variable_id, unit_id, tms_meta):


    """

    :param session:
    :param engine:
    :param rainc_net_cdf_file_path:
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

    if not os.path.exists(rainc_net_cdf_file_path):
        logger.warning('no rainc netcdf')
        print('no rainc netcdf')
    elif not os.path.exists(rainnc_net_cdf_file_path):
        logger.warning('no rainnc netcdf')
        print('no rainnc netcdf')
    else:

        """
        RAINC netcdf data extraction
        """
        nc_fid = Dataset(rainc_net_cdf_file_path, mode='r')

        time_unit_info = nc_fid.variables['XTIME'].units

        time_unit_info_list = time_unit_info.split(' ')

        lats = nc_fid.variables['XLAT'][0, :, 0]
        lons = nc_fid.variables['XLONG'][0, 0, :]

        lon_min = lons[0].item()
        lat_min = lats[0].item()
        lon_max = lons[-1].item()
        lat_max = lats[-1].item()
        print('[lon_min, lat_min, lon_max, lat_max] :', [lon_min, lat_min, lon_max, lat_max])

        lat_inds = np.where((lats >= lat_min) & (lats <= lat_max))
        lon_inds = np.where((lons >= lon_min) & (lons <= lon_max))

        rainc = nc_fid.variables['RAINC'][:, lat_inds[0], lon_inds[0]]

        """
        RAINNC netcdf data extraction
        """
        nnc_fid = Dataset(rainnc_net_cdf_file_path, mode='r')

        rainnc = nnc_fid.variables['RAINNC'][:, lat_inds[0], lon_inds[0]]

        times = nc_fid.variables['XTIME'][:]

        ts_start_date = datetime.strptime(time_unit_info_list[2], '%Y-%m-%dT%H:%M:%S')
        ts_end_date = datetime.strptime(time_unit_info_list[2], '%Y-%m-%dT%H:%M:%S') + timedelta(
                            minutes=float(max(times)))

        start_date = datetime_utc_to_lk(ts_start_date, shift_mins=0).strftime('%Y-%m-%d %H:%M:%S')
        end_date = datetime_utc_to_lk(ts_end_date, shift_mins=0).strftime('%Y-%m-%d %H:%M:%S')

        prcp = rainc + rainnc

        nc_fid.close()
        nnc_fid.close()

        diff = get_two_element_average(prcp)

        width = len(lons)
        height = len(lats)

        for y in range(height):
            for x in range(width):

                lat = float(lats[y])
                lon = float(lons[x])

                tms_meta['latitude'] = str(lat)
                tms_meta['longitude'] = str(lon)

                station_prefix = '{}_{}'.format(lat, lon)

                station_id = get_station_id(session=session, latitude=lat, longitude=lon, station_type=StationEnum.WRF)
                if station_id is None:
                    logger.info("Adding station {} to the station table in the database".format(station_prefix))
                    add_station(session=session, name=station_prefix, latitude=lat, longitude=lon,
                            description="WRF point",
                            station_type=StationEnum.WRF)
                    station_id = get_station_id(session=session, latitude=lat, longitude=lon,
                            station_type=StationEnum.WRF)

                ts = Timeseries(session)

                tms_id = ts.get_timeseries_id_if_exists(tms_meta)
                logger.info("Existing timeseries id: {}".format(tms_id))

                if tms_id is None:
                    tms_id = ts.generate_timeseries_id(tms_meta)
                    logger.info('HASH SHA256 created: {}'.format(tms_id))

                    run = { 'id'      : tms_id, 'sim_tag': tms_meta['sim_tag'], 'start_date': start_date,
                            'end_date': end_date, 'station': station_id, 'source': source_id, 'variable': variable_id,
                            'unit'    : unit_id, 'fgt': None, 'scheduled_date': tms_meta["scheduled_date"]
                            }

                    data_list = []
                    # generate timeseries for each station
                    for i in range(len(diff)):
                        data = {}
                        ts_time = datetime.strptime(time_unit_info_list[2], '%Y-%m-%dT%H:%M:%S') + timedelta(
                                minutes=times[i].item())
                        t = datetime_utc_to_lk(ts_time, shift_mins=0)
                        data['id'] = tms_id
                        data['time'] = t.strftime('%Y-%m-%d %H:%M:%S')
                        data['value'] = diff[i, y, x]
                        data_list.append(data)

                else:
                    logger.info("Timseries id already exists in the database : {}".format(tms_id))
                    logger.info("For the meta data : {}".format(tms_meta))

                push_rainfall_to_db(session=session, engine=engine, ts_data=data_list, ts_run=run)


def init(session, model, wrf_model_list, version, variable, unit, unit_type):
    for _wrf_model in wrf_model_list:
        source_name = "{}_{}".format(model, _wrf_model)
        if get_source_id(session=session, model=source_name, version=version) is None:
            add_source(session=session, model=source_name, version=version, parameters=None)

    if get_variable_id(session=session, variable=variable) is None:
        add_variable(session=session, variable=variable)

    if get_unit_id(session=session, unit=unit, unit_type=unit_type) is None:
        add_unit(session=session, unit=unit, unit_type=unit_type)


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
    rainc_net_cdf_file :  RAINC_2019-03-23_A.nc
    rainnc_net_cdf_file :  RAINNC_2019-03-23_A.nc
    rainc_net_cdf_file_path :  /mnt/disks/wrf-mod/STATIONS_2019-03-23/RAINC_2019-03-23_A.nc
    rainnc_net_cdf_file_path :  /mnt/disks/wrf-mod/STATIONS_2019-03-23/RAINNC_2019-03-23_A.nc    

    tms_meta = {
                    'sim_tag'       : sim_tag,
                    'scheduled_date': scheduled_date,
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
        if 'wrf_dir' in config and (config['wrf_dir'] != ""):
            wrf_dir = config['wrf_dir']
        else:
            logger.error("wrf_dir not specified in config file.")
            exit(1)

        if 'model' in config and (config['model'] != ""):
            model = config['model']
        else:
            logger.error("model not specified in config file.")
            exit(1)

        if 'version' in config and (config['version'] != ""):
            version = config['version']
        else:
            logger.error("version not specified in config file.")
            exit(1)

        if 'wrf_model_list' in config and (config['wrf_model_list'] != ""):
            wrf_model_list = config['wrf_model_list']
            wrf_model_list = wrf_model_list.split(',')
        else:
            logger.error("wrf_model_list not specified in config file.")
            exit(1)

        # unit details
        if 'unit' in config and (config['unit'] != ""):
            unit = config['unit']
        else:
            logger.error("unit not specified in config file.")
            exit(1)

        if 'unit_type' in config and (config['unit_type'] != ""):
            unit_type = UnitType.getType(config['unit_type'])
        else:
            logger.error("unit_type not specified in config file.")
            exit(1)

        # variable details
        if 'variable' in config and (config['variable'] != ""):
            variable = config['variable']
        else:
            logger.error("variable not specified in config file.")
            exit(1)

        # connection params
        if 'host' in config and (config['host'] != ""):
            host = config['host']
        else:
            logger.error("host not specified in config file.")
            exit(1)

        if 'user' in config and (config['user'] != ""):
            user = config['user']
        else:
            logger.error("user not specified in config file.")
            exit(1)

        if 'password' in config and (config['password'] != ""):
            password = config['password']
        else:
            logger.error("password not specified in config file.")
            exit(1)

        if 'db' in config and (config['db'] != ""):
            db = config['db']
        else:
            logger.error("db not specified in config file.")
            exit(1)

        if 'port' in config and (config['port'] != ""):
            port = config['port']
        else:
            logger.error("port not specified in config file.")
            exit(1)

        if 'start_date' in config and (config['start_date'] != ""):
            run_date_str = config['start_date']
            scheduled_date = (datetime.strptime(run_date_str, '%Y-%m-%d') + timedelta(days=1))\
                .strftime('%Y-%m-%d 06:45:00')
        else:
            run_date_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            scheduled_date = datetime.strftime(datetime.now(), '%Y-%m-%d 06:45:00')

        daily_dir = 'STATIONS_{}'.format(run_date_str)

        output_dir = os.path.join(wrf_dir, daily_dir)

        engine = get_engine(dialect=DIALECT_MYSQL, driver=DRIVER_PYMYSQL, host=host, user=user,
                password=password, db=db, port=port)
        logger.info("Connecting to database : dialect={}, driver={}, host={}, user={}, password={}, db={}, port={}"
            .format(DIALECT_MYSQL, DRIVER_PYMYSQL, host, user, password, db, port))

        Session = get_sessionmaker(engine=engine)

        session = Session()

        init(session, model, wrf_model_list, version, variable, unit, unit_type)

        variable_id = get_variable_id(session=session, variable=variable)
        unit_id = get_unit_id(session=session, unit=unit, unit_type=unit_type)

        for wrf_model in wrf_model_list:
            rainc_net_cdf_file = 'RAINC_{}_{}.nc'.format(run_date_str, wrf_model)
            rainnc_net_cdf_file = 'RAINNC_{}_{}.nc'.format(run_date_str, wrf_model)

            rainc_net_cdf_file_path = os.path.join(output_dir, rainc_net_cdf_file)
            logger.info("rainc_net_cdf_file_path : {}".format(rainc_net_cdf_file_path))

            rainnc_net_cdf_file_path = os.path.join(output_dir, rainnc_net_cdf_file)
            logger.info("rainnc_net_cdf_file_path : {}".format(rainnc_net_cdf_file_path))

            sim_tag = 'evening_18hrs'
            source_name = "{}_{}".format(model, wrf_model)
            source_id = get_source_id(session=session, model=source_name, version=version)

            tms_meta = {
                    'sim_tag'       : sim_tag,
                    'scheduled_date': scheduled_date,
                    'model'         : source_name,
                    'version'       : version,
                    'variable'      : variable,
                    'unit'          : unit,
                    'unit_type'     : unit_type.value
                    }

            try:
                read_netcdf_file(session=session,
                        rainc_net_cdf_file_path=rainc_net_cdf_file_path,
                        rainnc_net_cdf_file_path=rainnc_net_cdf_file_path,
                        source_id=source_id, variable_id=variable_id, unit_id=unit_id, tms_meta=tms_meta)
            except Exception as e:
                logger.error("Net CDF file reading error.")
                print('Net CDF file reading error.')
                traceback.print_exc()

        try:
            ts = Timeseries(session)
            fgt = datetime_utc_to_lk(datetime.now(), shift_mins=0).strftime('%Y-%m-%d %H:%M:%S')
            ts.update_fgt(scheduled_date=scheduled_date, fgt=fgt)
        except Exception as e:
            logger.error('Exception occurred while updating fgt')
            print('Exception occurred while updating fgt')
            traceback.print_exc()

    except Exception as e:
        logger.error('JSON config data loading error.')
        print('JSON config data loading error.')
        traceback.print_exc()
    finally:
        logger.info("Process finished.")
        print("Process finished.")
