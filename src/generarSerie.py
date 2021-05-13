import glob
import pandas as pd
import wrf
import re
import numpy as np
import argparse
from datetime import datetime
from netCDF4 import Dataset
from wrf import ll_to_xy


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


def getT2product(dfT2, dfTSK):
    """ Obtiene un pronostico de temperatura a partir de las variables
    T2 y TSK
    """
    mask = dfTSK['TSK'].values - dfT2['T2'].values
    mask = mask > 0
    maskinverted = np.invert(mask)

    fieldname = "T2P"
    dfT2 = dfT2.rename(columns={'T2': fieldname})
    dfTSK = dfTSK.rename(columns={'TSK': fieldname})

    dfT2['date'] = dfT2.index
    append = dfT2[mask].append(dfTSK[maskinverted], sort=True)
    append.sort_index(inplace=True)

    append = append[["T2P", 'date']]
    return append


def extraerWrfoutSerie(file_paths: str, variable: str, x: int, y: int):
    """ extrae de los arechivos wrfout listados en file_paths
    para la posicion (x, y) toda la serie de la variable
    seleccionada"""

    dfData = pd.DataFrame()

    for f in file_paths:
        print(f'Processing: {f}')

        try:
            wrf_temp = Dataset(f)
        except OSError:
            continue

        t2 = wrf.getvar(wrf_temp, variable, timeidx=wrf.ALL_TIMES)
        t2_ema = t2[:, y, x]

        dfT2ema = pd.DataFrame(t2_ema.to_pandas(), columns=[variable])
        if variable == 'T2':
            dfT2ema['T2'] = dfT2ema['T2'] - 273.15
            tTSK = wrf.getvar(wrf_temp, 'TSK', timeidx=wrf.ALL_TIMES)
            tTSK_ema = tTSK[:, y, x]
            dfTSKema = pd.DataFrame(tTSK_ema.to_pandas(), columns=['TSK'])
            dfTSKema['TSK'] = dfTSKema['TSK'] - 273.15
            dfT2ema = getT2product(dfT2ema, dfTSKema)
        wrf_temp.close()
        dfData = pd.concat([dfData, dfT2ema[9:33]])

    return dfData


def guardarPickle(dfTmp, filename: str):
    """ guarda un pickle del dataframe dfTemp """

    dfTmp.to_pickle(f'pickles/{filename}')


def generar_serie(path: str, lat: float, lon: float, inicio: str, fin: str,
                  variable: str, loca: str, param: str, run: str):

    lista = obtenerListaArchivos(path)

    x, y = getXeY(lista[0], lat, lon)

    lista_filtrada = filter_by_dates(lista, inicio, fin)

    dfData = extraerWrfoutSerie(lista_filtrada, variable, x, y)

    guardarPickle(dfData, f'{variable}_{param}_{run}_{loca}')


def main():
    base = '/home/datos/wrfdatos/wrfout/20*_*/'

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

    for param in ['A', 'B', 'C', 'D']:
        path = f'{base}wrfout_{param}_d01_*_{args.run}:00:00'
        generar_serie(path, args.lat, args.lon, args.inicio, args.fin,
                      args.variable, args.loca, param, args.run)


if __name__ == "__main__":
    main()
