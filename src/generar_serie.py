import glob
import pandas as pd
import wrf
import re
import argparse
import ray
from libproc.genproc import getT2product
from datetime import datetime
from netCDF4 import Dataset
from wrf import ll_to_xy


ray.init(address='auto', _redis_password='5241590000000000')


def obtenerListaArchivos(path: str):
    """ genera una lista de los archivos alojados en str """

    lista = glob.glob(path, recursive=True)

    return lista


def getXeY(wrfout: str, lat: float, lon: float):
    """ Obtiene los x e y del lat long
    """
    try:
        wrf_file = Dataset(wrfout)
    except OSError:
        print("No hay wrfout")

    (x, y) = ll_to_xy(wrf_file, lat, lon)

    return int(x), int(y)


def filter_by_dates(file_list, start_date, end_date, extra_filter=None):
    """filter files names in 'file_list' that belongs to creation dates
    betweeen [start_date, end_date)
    start_date and end_date are given as YYYY-MM-DD"""

    filtered_dates = list()
    sd = datetime.strptime(start_date, '%Y-%m-%d').date()  # start date
    ed = datetime.strptime(end_date, '%Y-%m-%d').date()   # end date
    for name in file_list:
        match = re.search('\d{4}-\d{2}-\d{2}', name)
        date = datetime.strptime(match.group(), '%Y-%m-%d').date()
        if sd <= date <= ed:
            filtered_dates.append(name)
    return filtered_dates


@ray.remote
def extraerWrfoutSerie(file_paths: str, variable: str, x: int, y: int, loca: str, par: str, run: str):
    """ extrae de los arechivos wrfout listados en file_paths
    para la posicion (x, y) toda la serie de la variable
    seleccionada"""

    print(f'Processing: {file_paths}')

    try:
        wrf_temp = Dataset(file_paths)
    except OSError:
        return
    try:
        var = wrf.getvar(wrf_temp, variable, timeidx=wrf.ALL_TIMES)
    except RuntimeError:
        print("Corrupted file")
        return

    var_ema = var[:, y, x]
    dfVARema = pd.DataFrame(var_ema.to_pandas(), columns=[variable])
    if variable == 'T2':
        dfVARema['T2'] = dfVARema['T2'] - 273.15
        tTSK = wrf.getvar(wrf_temp, 'TSK', timeidx=wrf.ALL_TIMES)
        tTSK_ema = tTSK[:, y, x]
        dfTSKema = pd.DataFrame(tTSK_ema.to_pandas(), columns=['TSK'])
        dfTSKema['TSK'] = dfTSKema['TSK'] - 273.15
        dfVARema = getT2product(dfVARema, dfTSKema)
    wrf_temp.close()
    wrf_temp = 0
    dfData = dfVARema[9:33]
    dfData.to_csv(f'{variable}_{loca}_{run}_{par}.csv', mode='a', header=None, index=False)
    dfData = 0


def guardarPickle(dfTmp, filename: str):
    """ guarda un pickle del dataframe dfTemp """

    dfTmp.to_pickle(f'pickles/{filename}')


def generar_serie(path: str, lat: float, lon: float, inicio: str, fin: str,
                  variable: str, loca: str, param: str, run: str, path_old):

    lista = obtenerListaArchivos(path)
    if path_old != 0:
        lista.append(obtenerListaArchivos(path_old))

    x, y = getXeY(lista[0], lat, lon)

    lista_filtrada = filter_by_dates(lista, inicio, fin)

    if not lista_filtrada:
        print('no matced dates')
        return

    it = ray.util.iter.from_items(lista_filtrada)

    dfData_ = [extraerWrfoutSerie.remote(filename, variable, x, y, loca, param, run) for filename in it.gather_async()]
    ray.get(dfData_)

    # guardarPickle(dfData, f'{variable}_{param}_{run}_{loca}')


def main():
    base = '/home/datos/wrf/wrfout/20*_*/'

    parser = argparse.ArgumentParser(prog="Obtener variable puntual WRF,\
                                           generarSerie.py")

    parser.add_argument("variable",
                        help="variable con la nomenclatura de  WRF")
    parser.add_argument("inicio",
                        help="fecha de inicio: ej 2019-06-01")
    parser.add_argument("fin",
                        help="fecha de inicio: ej 2020-07-31")
    parser.add_argument("lat",
                        help="latitud")
    parser.add_argument("lon",
                        help="longitud")
    parser.add_argument("loca",
                        help="referencia sobre le punto: ej. localidad")
    parser.add_argument("run",
                        help="corrida de WRF")

    args = parser.parse_args()

    for param in ['A']:
        path = f'{base}wrfout_{param}_d01_*_{args.run}:00:00'
        path_old = 0
        if (datetime.strptime(args.inicio, '%Y-%m-%d').date() < datetime.strptime('2018-09-01', '%Y-%m-%d').date()):
            path_old = f'{base}wrfout_d01_*_{args.run}:00:00'
        generar_serie(path, args.lat, args.lon, args.inicio, args.fin,
                      args.variable, args.loca, param, args.run, path_old)


if __name__ == "__main__":
    main()
