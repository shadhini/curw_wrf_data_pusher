import traceback
import pymysql
from db_adapter.csv_utils import read_csv

from db_adapter.base import get_Pool, destroy_Pool
from db_adapter.constants import CURW_FCST_HOST, CURW_FCST_USERNAME, CURW_FCST_PASSWORD, CURW_FCST_PORT, CURW_FCST_DATABASE
from db_adapter.curw_fcst.timeseries import Timeseries


def import_old_data():
    # Connect to the database
    pool = get_Pool(host=CURW_FCST_HOST, user=CURW_FCST_USERNAME, password=CURW_FCST_PASSWORD, port=CURW_FCST_PORT, db=CURW_FCST_DATABASE)

    connection = pool.connection()

    curw_fcst_new_to_old_hash_id_mapping = read_csv("curw_fcst_new_to_old_hash_id_mapping.csv")

    TS = Timeseries(pool=pool)

    try:

        for hash_index in range(len(curw_fcst_new_to_old_hash_id_mapping)):
            print("##### Hash index: ", hash_index, " #####")
            fgt_list = []
            # Extract fgts
            with connection.cursor() as cursor1:
                sql_statement = "select distinct `fgt` from `data_v3` where `id`=%s order by `fgt` desc;"
                cursor1.execute(sql_statement, curw_fcst_new_to_old_hash_id_mapping[hash_index][1])
                fgts = cursor1.fetchall()
                for fgt in fgts:
                    fgt_list.append(fgt.get('fgt'))

            for fgt in fgt_list:
                timeseries= []
                with connection.cursor() as cursor2:
                    sql_statement = "select * from `data_v3` where `id`=%s and `fgt`=%s;"
                    cursor2.execute(sql_statement, (curw_fcst_new_to_old_hash_id_mapping[hash_index][1], fgt))
                    results = cursor2.fetchall()
                    for result in results:
                        timeseries.append([curw_fcst_new_to_old_hash_id_mapping[hash_index][0],
                                           result.get('time'), result.get('fgt'), result.get('value')])

                TS.insert_data(timeseries=timeseries, upsert=True)
                TS.update_start_date(id_=curw_fcst_new_to_old_hash_id_mapping[hash_index][0], start_date=fgt)

    except Exception as ex:
        traceback.print_exc()
    finally:
        connection.close()
        destroy_Pool(pool=pool)
        print()


import_old_data()


