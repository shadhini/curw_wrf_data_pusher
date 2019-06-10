import operator
import collections
import csv
import pymysql
import traceback

from math import acos, cos, sin, radians


def read_csv(file_name):
    """
    Read csv file
    :param file_name: <file_path/file_name>.csv
    :return: list of lists which contains each row of the csv file
    """

    with open(file_name, 'r') as f:
        data = [list(line) for line in csv.reader(f)][1:]

    return data


def create_csv(file_name, data):
    """
    Create new csv file using given data
    :param file_name: <file_path/file_name>.csv
    :param data: list of lists
    e.g. [['Person', 'Age'], ['Peter', '22'], ['Jasmine', '21'], ['Sam', '24']]
    :return:
    """
    with open(file_name, 'w') as csvFile:
        writer = csv.writer(csvFile)
        writer.writerows(data)

    csvFile.close()


def wrf_new_to_wrf_old_station_id_mapping(new_wrf_csv, old_wrf_csv):

    new_wrf_grids = read_csv(new_wrf_csv)

    old_wrf_grids = read_csv(old_wrf_csv)

    wrf_new_to_old_id_mapping_list = [['new_wrf_id', 'old_wrf_id', 'dist']]

    for origin_index in range(len(new_wrf_grids)):

        wrf_new_to_old_id_mapping = [new_wrf_grids[origin_index][0]]

        origin_lat = float(new_wrf_grids[origin_index][2])
        origin_lng = float(new_wrf_grids[origin_index][3])

        distances = {}

        for old_index in range(len(old_wrf_grids)):
            lat = float(old_wrf_grids[old_index][2])
            lng = float(old_wrf_grids[old_index][3])

            intermediate_value = cos(radians(origin_lat)) * cos(radians(lat)) * cos(radians(lng) - radians(origin_lng)) + sin(radians(origin_lat)) * sin(radians(lat))
            if intermediate_value < 1:
                distance = 6371 * acos(intermediate_value)
            else:
                distance = 6371 * acos(1)

            distances[old_wrf_grids[old_index][0]] = distance

        sorted_distances = collections.OrderedDict(sorted(distances.items(), key=operator.itemgetter(1))[:10])

        count = 0
        for key in sorted_distances.keys():
            if count < 1:
                wrf_new_to_old_id_mapping.extend([key, sorted_distances.get(key)])
                count += 1
            else:
                break

        print(wrf_new_to_old_id_mapping)
        wrf_new_to_old_id_mapping_list.append(wrf_new_to_old_id_mapping)

    create_csv('wrf_new_to_old_station_id_mapping.csv', wrf_new_to_old_id_mapping_list)


# wrf_new_to_wrf_old_station_id_mapping(new_wrf_csv="wrf_stations.csv", old_wrf_csv="outdated_wrf_stations.csv")

def curw_fcst_new_to_old_hash_id_mapping():

    curw_fcst_new_to_old_hash_id_mapping_list = [['new_hash_id', 'old_hash_id']]

    try:

        # Connect to the database
        connection = pymysql.connect(host='35.230.102.148',
                user='root',
                password='cfcwm07',
                db='curw_fcst',
                cursorclass=pymysql.cursors.DictCursor)

        for i in range(8):  # source id

            old_hash_ids = read_csv('old_source{}_run.csv'.format(i+1))
            # id,sim_tag,start_date,end_date,station,source,variable,unit

            for old_index in range(len(old_hash_ids)):
                # Extract station ids
                with connection.cursor() as cursor1:
                    sql_statement = "SELECT `id` FROM `run` WHERE `source`=%s AND `station`=%s AND " \
                                    "`sim_tag`='evening_18hrs' AND `variable`=1 AND `unit`=1;"
                    cursor1.execute(sql_statement, (old_hash_ids[old_index][5], old_hash_ids[old_index][4]))
                    new_id = cursor1.fetchone()['id']

                    print([new_id, old_hash_ids[old_index][0]])
                    curw_fcst_new_to_old_hash_id_mapping_list.append([new_id, old_hash_ids[old_index][0]])

        create_csv('curw_fcst_new_to_old_hash_id_mapping.csv', curw_fcst_new_to_old_hash_id_mapping_list)

    except Exception as ex:
        traceback.print_exc()
    finally:
        connection.close()


curw_fcst_new_to_old_hash_id_mapping()

