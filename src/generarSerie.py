import glob
import pandas as pd
import wrf
import re
from datetime import date, timedelta, datetime
from netCDF4 import Dataset

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


def obtenerListaArchivos(path: str):
    """ genera una lista de los archivos alojados en str """

    lista = glob.glob(path, recursive=True)

    return lista


def extraerWrfoutSerie(file_paths: str, x: int, y: int):
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
     
        t2 = wrf.getvar(wrf_temp, "T2", timeidx=wrf.ALL_TIMES)
        wrf_temp.close()
        t2_ubp = t2[:, y, x]

        dfT2ubp = pd.DataFrame(t2_ubp.to_pandas(), columns=['T2'])
        dfT2ubp['T2'] = dfT2ubp['T2'] - 273.15

        dfData = pd.concat([dfData, dfT2ubp[9:33]])

    return dfData


def guardarPickle(dfTmp, filename: str):
    """ guarda un pickle del dataframe dfTemp """

    dfTmp.to_pickle(f'{filename}')


def generar_serie(path: str, lat: float, lon: float, variabe: str, param: str):

    lista = obtenerListaArchivos(path)

    x, y = getXeY(lista[0], lat, lon)

    lista_filtrada = filter_by_dates(lista, '2019-06-01', '2020-07-31')

    dfData = extraerWrfoutSerie(lista_filtrada, x, y)

    guardarPickle(dfData, f'{variable}_{param}_VR')


def getXeY(wrfout: str, lat: float, lon: float):
    """ Obtiene los x e y del lat long 
    """
    try:
        wrfnc = Dataset(wrfout)
    except OSError:
            continue

    (x, y) = ll_to_xy(wrf_file, lat_ubp, long_ubp)

    return int(x), int(y)


def main():
    parser = argparse.ArgumentParser(prog="Obtener variable puntual WRF")
    
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
    parser.add_argument("ref",
                        help="referencia sobre le punto: ej. localidad")
    parser.add_argument("run",
                        help="corrida de WRF")

    args = parser.parse_args()

    base = '/home/datos/wrfdatos/wrfout/20*_*/'

    for param in ['A', 'B', 'C', 'D']:
        path = f'{base}wrfout_{param}_d01_*_{args.run}:00:00'
        generar_serie(path, args.lat, args.lon, args.variable, param)
 

 if __name__ == "__main__":
    main()
    
