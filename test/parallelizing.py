import multiprocessing as mp
from datetime import datetime, timedelta


def write_to_file(file_name, data, string):
    print(string)
    print(datetime.now())
    with open(file_name, 'w+') as f:
        f.write('\n'.join(data))
        f.write(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))


pool = mp.Pool(mp.cpu_count())

data = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10']
string = "njnlkn"

wrf_models = ['A.txt', 'B.txt', 'C.txt', 'E.txt']

pool.starmap_async(write_to_file, [(wrf_model, data, string) for wrf_model in wrf_models])

pool.close()


