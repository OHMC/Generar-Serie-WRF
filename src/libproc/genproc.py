import numpy as np


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

    
    append = dfT2[mask].append(dfTSK[maskinverted], sort=True)
    append.sort_index(inplace=True)

    return append
