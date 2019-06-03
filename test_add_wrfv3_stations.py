from db_adapter.curw_fcst.station import add_wrf_stations, get_wrf_stations
from db_adapter.base import get_Pool

# connection params

USERNAME = "root"
PASSWORD = "password"
HOST = "127.0.0.1"
PORT = 3306
DATABASE = "test_schema2"

pool = get_Pool(host=HOST, port=PORT, user=USERNAME, password=PASSWORD, db=DATABASE)
# add_wrfv3_stations(pool)

wrfv3_stations = get_wrf_stations(pool)

print(wrfv3_stations.get('5.722969055175781_79.5214614868164'))
print(wrfv3_stations.get('10.064254760742188_82.1899185180664'))


a = 78.5692316887867534
print(float('%.6f' % a))