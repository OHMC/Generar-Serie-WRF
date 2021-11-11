import glob
import pandas as pd
import wrf
import re
import argparse
import ray
from libproc.genproc import getT2product
from config.constants import variables_list
from datetime import datetime
from netCDF4 import Dataset
from wrf import ll_to_xy


ray.init(address='auto', _redis_password='5241590000000000')


def obtenerListaArchivos(path: str):
    """ genera una lista de los archivos alojados en str """

    lista = glob.glob(path, recursive=True)

    return lista


def getXeY_from_list(wrfout: str, estaciones: pd.DataFrame):
    """ Obtiene los x e y del lat long
    """
    try:
        wrf_file = Dataset(wrfout)
    except OSError:
        print("No hay wrfout")

    lista_x = []
    lista_y = []
    for key, estacion in estaciones.iterrows():

        (x, y) = ll_to_xy(wrf_file, float(estacion.LAT), float(estacion.LONG))
        x = int(x)
        y = int(y)
        lista_x.append(x)
        lista_y.append(y)

    estaciones['x'] = lista_x
    estaciones['y'] = lista_y

    return estaciones


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
def extraerWrfoutSerie(file_paths, latlong_estaciones, par, run):
    """ extrae de los arechivos wrfout listados en file_paths
    para la posicion (x, y) toda la serie de la variable
    seleccionada"""

    print(f'Processing: {file_paths}')

    try:
        wrf_temp = Dataset(file_paths)
    except OSError:
        return
    for var_name in variables_list:

        try:
            var = wrf.getvar(wrf_temp, var_name, timeidx=wrf.ALL_TIMES)
        except RuntimeError:
            print("Corrupted file")
            return
        except IndexError:
            print("Corrupted file, no index")
            return

        for key, estacion in latlong_estaciones.iterrows():
            var_ema = var[:, int(estacion.y), int(estacion.x)]

            dfVARema = pd.DataFrame(var_ema.to_pandas(), columns=[var_name])
            # dfVARema['T2'] = dfVARema['T2'] - 273.15
            
            if var_name == 'T2':
                dfVARema['T2'] = dfVARema['T2'] - 273.15
                tTSK = wrf.getvar(wrf_temp, 'TSK', timeidx=wrf.ALL_TIMES)
                tTSK_ema = tTSK[:, estacion.y, estacion.x]
                dfTSKema = pd.DataFrame(tTSK_ema.to_pandas(), columns=['TSK'])
                dfTSKema['TSK'] = dfTSKema['TSK'] - 273.15
                dfVARema = getT2product(dfVARema, dfTSKema)
                var_name_print = 'T2P'
            
            dfData = dfVARema[9:33]
            dfData.to_csv(f'csv_output/{estacion.NOMBRE}_{var_name_print}_{run}_{par}.csv', mode='a', header=None)
            dfData = 0

    wrf_temp.close()
    wrf_temp = 0


@ray.remote
def extraer_serie_por_corrida(file_paths, latlong_estaciones, par, run):
    """ extrae de los arechivos wrfout listados en file_paths
    para la posicion (x, y) toda la serie de la variable
    seleccionada"""

    print(f'Processing este: {file_paths}') 

    try:
        wrf_temp = Dataset(file_paths)
    except OSError:
        return
    for var_name in variables_list:

        try:
            var = wrf.getvar(wrf_temp, var_name, timeidx=wrf.ALL_TIMES)
        except RuntimeError:
            print("Corrupted file")
            return
        except IndexError:
            print("Corrupted file, no index")
            return

        for key, estacion in latlong_estaciones.iterrows():
            var_ema = var[:, int(estacion.y), int(estacion.x)]

            dfVARema = pd.DataFrame(var_ema.to_pandas(), columns=[var_name])
            dfVARema['T2'] = dfVARema['T2'] - 273.15
            """
            if var_name ==  'T2':
                dfVARema['T2'] = dfVARema['T2'] - 273.15
                tTSK = wrf.getvar(wrf_temp, 'TSK', timeidx=wrf.ALL_TIMES)
                tTSK_ema = tTSK[:, estacion.y, estacion.x]
                dfTSKema = pd.DataFrame(tTSK_ema.to_pandas(), columns=['TSK'])
                dfTSKema['TSK'] = dfTSKema['TSK'] - 273.15
                dfVARema = getT2product(dfVARema, dfTSKema)
                var_name_print = 'T2P'
            """
            var_name_print = 'T2'
            timestamp = dfVARema.index[0]  # to get rundate
            dfVARema.reset_index(drop=True, inplace=True)
            dfVARema.rename(columns={f'{var_name_print}': f"{timestamp}"}, inplace=True)
            dfVARema.T.to_csv(f'csv_output/{estacion.NOMBRE}_{var_name_print}_{run}_{par}.csv', mode='a', header=None)
            dfVARema = 0

    wrf_temp.close()
    wrf_temp = 0


def guardarPickle(dfTmp, filename: str):
    """ guarda un pickle del dataframe dfTemp """

    dfTmp.to_pickle(f'pickles/{filename}')


@ray.remote
def print_files(filename, latlong_estaciones, param, run):
    print(f"f: {filename} - p: {param}")


def generar_serie(path: str, inicio: str, fin: str,
                  param: str, run: str, path_old, select_serie):

    latlong_estaciones = pd.read_csv('csvs/estaciones_faltantes.csv')
    base = '/home/datos/wrfout/20*_*/'

    lista = obtenerListaArchivos(f"{base}{path}")

    if path_old != 0:
        lista.extend(obtenerListaArchivos(f"{base}{path_old}"))

    lista_filtrada = filter_by_dates(lista, inicio, fin)

    if not lista_filtrada:
        print('no matced dates')
        return
    print("Extrating data...")

    latlong_estaciones = getXeY_from_list(lista_filtrada[0], latlong_estaciones)

    print(latlong_estaciones)

    it = ray.util.iter.from_items(lista_filtrada)
    print(f"select_serie {select_serie}")
    if select_serie in 'None':
        print("Here we gooo Extraer serie")
        dfData_ = [extraer_serie_por_corrida.remote(filename, latlong_estaciones, param, run) for filename in it.gather_async()]
        ray.get(dfData_)
    else:
        print("Generar Serie")
        dfData_ = [extraerWrfoutSerie.remote(filename, latlong_estaciones, param, run) for filename in it.gather_async()]
        ray.get(dfData_)
    # for filename in lista_filtrada:
    #    extraerWrfoutSerie(filename , latlong_estaciones, param, run)

    # guardarPickle(dfData, f'{variable}_{param}_{run}_{loca}')


def main():
    parser = argparse.ArgumentParser(prog="Obtener variable puntual WRF,\
                                           generarSerie.py")

    parser.add_argument("inicio",
                        help="fecha de inicio: ej 2019-06-01")
    parser.add_argument("fin",
                        help="fecha de inicio: ej 2020-07-31")
    parser.add_argument("run",
                        help="corrida de WRF")
    parser.add_argument("gen_serie",
                        help="si se genera una serie o datos por corrida",
                        default=None)

    args = parser.parse_args()

    for param in ['A']:
        path = f'wrfout_{param}_d01_*_{args.run}:00:00*'
        path_old = 0
        if (datetime.strptime(args.inicio, '%Y-%m-%d').date() < datetime.strptime('2018-09-01', '%Y-%m-%d').date()):
            path_old = f'wrfout_d01_*_{args.run}:00:00'
        generar_serie(path, args.inicio, args.fin, param, args.run, path_old, args.gen_serie)


if __name__ == "__main__":
    main()
