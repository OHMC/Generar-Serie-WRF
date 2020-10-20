import pandas as pd
import wrf
import re
import argparse
from datetime import datetime
from netCDF4 import Dataset
from wrf import ll_to_xy


def getXeY(wrf_file: str, lat: float, lon: float):
    """ Obtiene los x e y del lat long
    """
    (x, y) = ll_to_xy(wrf_file, lat, lon)

    return int(x), int(y)


def extraerWrfoutSerie(wrf_file: str, variable: str, x: int, y: int):
    """ extrae de los arechivos wrfout listados en file_paths
    para la posicion (x, y) toda la serie de la variable
    seleccionada"""

    dfData = pd.DataFrame()

    print(f'Processing: {variable}')

    t2 = wrf.getvar(wrf_file, variable, timeidx=wrf.ALL_TIMES)
    t2_loc = t2[:, y, x]

    dfT2loc = pd.DataFrame(t2_loc.to_pandas(), columns=[variable])

    if variable == 'T2':
        dfT2loc['T2'] = dfT2loc['T2'] - 273.15

    dfData = pd.concat([dfData, dfT2loc[9:]])

    return dfData


def obtener_variable(wrfout: str, runtime: str,
                     variable: str, param: str):

    aws_list = pd.read_json('config/aws_list.json')

    try:
        wrf_file = Dataset(wrfout)
    except OSError:
        print(f"No existe el wrfout {wrfout}")
        return

    for index, values in aws_list.iterrows():
        x, y = getXeY(wrf_file, values['lat'], values['lon'])
        dfData = extraerWrfoutSerie(wrf_file, variable, x, y)
        pickleName = (f"{variable}_{param}_"
                      f"{runtime.strftime('%Y-%m-%dT%H')}_{values['short']}")
        guardarPickle(dfData, pickleName)

    wrf_file.close()


def guardarPickle(dfTmp, filename: str):
    """ guarda un pickle del dataframe dfTemp """

    dfTmp.to_pickle(f'pickles/{filename}')


def main():
    base = '/home/datos/wrfdatos/wrfout/'

    parser = argparse.ArgumentParser(prog="Obtener variable puntual WRF de UN dataset,\
                                           obtenerVariable.py")

    parser.add_argument("variable",
                        help="variable con la nomenclatura de  WRF")
    parser.add_argument("corrida",
                        help="fecha de la corrida: ej 2019-06-01T06")

    args = parser.parse_args()

    runtime = datetime.strptime(args.corrida, '%Y-%m-%dT%H')

    for param in ['A', 'B', 'C', 'D']:
        path = (f"{base}{runtime.strftime('%Y_%m')}/wrfout_{param}_d01_"
                f"{runtime.strftime('%Y-%m-%d')}_"
                f"{runtime.strftime('%H')}:00:00")
        obtener_variable(path, runtime, args.variable, param)


if __name__ == "__main__":
    main()
