import traceback
from netCDF4 import Dataset
import numpy as np
import os
from datetime import datetime, timedelta

from db_adapter.base import get_Pool

from db_adapter.curw_fcst.source import get_source_id, add_source
from db_adapter.curw_fcst.variable import get_variable_id, add_variable
from db_adapter.curw_fcst.unit import get_unit_id, add_unit, UnitType
from db_adapter.curw_fcst.station import StationEnum, get_station_id, add_station
from db_adapter.curw_fcst.timeseries import Timeseries

from logger import logger

SRI_LANKA_EXTENT = [79.5213, 5.91948, 81.879, 9.83506]


def push_rainfall_to_db(pool, ts_data, ts_run):
    """

    :param pool: database connection pool
    :param ts_data: timeseries
    :param ts_run: run entry
    :return:
    """
    ts = Timeseries(pool)

    try:
        ts.insert_timeseries(timeseries=ts_data, run_tuple=ts_run)
    except Exception:
        logger.error("Inserting the timseseries for tms_id {} failed.".format(ts_run[0]))
        traceback.print_exc()
        return False

    try:
        fgt = datetime_utc_to_lk(datetime.now(), shift_mins=0).strftime('%Y-%m-%d %H:%M:%S')
        ts.update_fgt(scheduled_date=scheduled_date, fgt=fgt)
    except Exception as e:
        logger.error('Exception occurred while updating fgt')
        print('Exception occurred while updating fgt')
        traceback.print_exc()
        return False


def get_two_element_average(prcp, return_diff=True):
    avg_prcp = (prcp[1:] + prcp[:-1]) * 0.5
    if return_diff:
        return avg_prcp - np.insert(avg_prcp[:-1], 0, [0], axis=0)
    else:
        return avg_prcp


def datetime_utc_to_lk(timestamp_utc, shift_mins=0):
    return timestamp_utc + timedelta(hours=5, minutes=30 + shift_mins)


def read_netcdf_file(pool, source_id, variable_id, unit_id, tms_meta):
    """

    :param pool: database connection pool
    :param source_id:
    :param variable_id:
    :param unit_id:
    :param tms_meta:
    :return:

    rainc_unit_info:  mm
    lat_unit_info:  degree_north
    time_unit_info:  minutes since 2019-04-02T18:00:00
    """

    if not os.path.exists("/home/shadhini/Documents/CUrW/NetCDF/RAINC_2019-03-17_A.nc"):
        logger.warning('no rainc netcdf')
        print('no rainc netcdf')
    elif not os.path.exists("/home/shadhini/Documents/CUrW/NetCDF/RAINNC_2019-03-17_A.nc"):
        logger.warning('no rainnc netcdf')
        print('no rainnc netcdf')
    else:

        """
        RAINC netcdf data extraction
        """
        nc_fid = Dataset("/home/shadhini/Documents/CUrW/NetCDF/RAINC_2019-03-17_A.nc", mode='r')

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
        nnc_fid = Dataset("/home/shadhini/Documents/CUrW/NetCDF/RAINNC_2019-03-17_A.nc", mode='r')

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

                add_station(pool=pool, name=station_prefix, latitude=lat, longitude=lon,
                        description="WRF point", station_type=StationEnum.WRF)
                station_id = get_station_id(pool=pool, latitude=lat, longitude=lon,
                        station_type=StationEnum.WRF)

                ts = Timeseries(pool)

                try:
                    tms_id = ts.generate_timeseries_id(tms_meta)
                    logger.info('HASH SHA256 created: {}'.format(tms_id))

                    run = (tms_id, tms_meta['sim_tag'], start_date, end_date, station_id, source_id, variable_id,
                           unit_id, None, tms_meta["scheduled_date"])
                    data_list = []
                    # generate timeseries for each station
                    for i in range(len(diff)):
                        ts_time = datetime.strptime(time_unit_info_list[2], '%Y-%m-%dT%H:%M:%S') + timedelta(
                                minutes=times[i].item())
                        t = datetime_utc_to_lk(ts_time, shift_mins=0)
                        data_list.append([tms_id, t.strftime('%Y-%m-%d %H:%M:%S'), float(diff[i, y, x])])

                    push_rainfall_to_db(pool=pool, ts_data=data_list, ts_run=run)
                except Exception as e:
                    print(e)
                    logger.info("Timseries id already exists in the database : {}".format(tms_id))
                    logger.info("For the meta data : {}".format(tms_meta))


def init(pool, version, variable, unit, unit_type):
    source_name = "WRF_TEST"
    add_source(pool=pool, model=source_name, version=version)

    add_variable(pool=pool, variable=variable)

    add_unit(pool=pool, unit=unit, unit_type=unit_type)


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
        # source details
        model = "WRF_A"
        version = "v3"

        # unit details
        unit = "mm"
        unit_type = UnitType.getType("Accumulative")

        # variable details
        variable = "Precipitation"

        scheduled_date = datetime.strftime(datetime.now(), '%Y-%m-%d 06:45:00')

        # connection params

        USERNAME = "root"
        PASSWORD = "password"
        HOST = "127.0.0.1"
        PORT = 3306
        DATABASE = "test_schema"

        pool = get_Pool(host=HOST, port=PORT, user=USERNAME, password=PASSWORD, db=DATABASE)

        init(pool=pool, version=version, variable=variable,
                unit=unit, unit_type=unit_type)

        variable_id = get_variable_id(pool=pool, variable=variable)
        unit_id = get_unit_id(pool=pool, unit=unit, unit_type=unit_type)

        sim_tag = 'evening_18hrs'
        source_id = get_source_id(pool=pool, model="WRF_TEST", version=version)

        tms_meta = {
                'sim_tag'       : sim_tag,
                'scheduled_date': scheduled_date,
                'model'         : "WRF_TEST",
                'version'       : version,
                'variable'      : variable,
                'unit'          : unit,
                'unit_type'     : unit_type.value
                }

        try:
            read_netcdf_file(pool=pool, source_id=source_id, variable_id=variable_id, unit_id=unit_id, tms_meta=tms_meta)
        except Exception as e:
            logger.error("Net CDF file reading error.")
            print('Net CDF file reading error.')
            traceback.print_exc()
        finally:
            pool.destroy()


    except Exception as e:
        logger.error('JSON config data loading error.')
        print('JSON config data loading error.')
        traceback.print_exc()
    finally:
        logger.info("Process finished.")
        print("Process finished.")
