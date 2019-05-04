import traceback
import json

from db_adapter.base import get_Pool

from db_adapter.curw_fcst.source import get_source_id, add_source
from db_adapter.curw_fcst.variable import get_variable_id, add_variable
from db_adapter.curw_fcst.unit import get_unit_id, add_unit, UnitType
from db_adapter.curw_fcst.station import add_wrfv3_stations

from logger import logger


def init(pool, model, wrf_model_list, version, variable, unit, unit_type):
    for _wrf_model in wrf_model_list:
        source_name = "{}_{}".format(model, _wrf_model)
        add_source(pool=pool, model=source_name, version=version)

    add_variable(pool=pool, variable=variable)

    add_unit(pool=pool, unit=unit, unit_type=unit_type)

    add_wrfv3_stations(pool)


if __name__=="__main__":

    try:
        config = json.loads(open('config.json').read())

        # source details
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

        pool = get_Pool(host=host, port=port, user=user, password=password, db=db)

        init(pool=pool, model=model, wrf_model_list=wrf_model_list, version=version, variable=variable,
                unit=unit, unit_type=unit_type)

    except Exception:
        logger.info("Initialization process failed.")
        traceback.print_exc()
    finally:
        pool.destroy()
        logger.info("Initialization process finished.")
