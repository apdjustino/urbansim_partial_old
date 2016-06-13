import orca
import pandas as pd
import numpy as np
from urbansim.utils.misc import reindex
import zones
import households
import parcels
import buildings

@orca.column('establishments', 'sector_id_six', cache=True)
def sector_id_six(establishments):
    e = establishments
    return 1*(e.sector_id==61) + 2*(e.sector_id==71) + 3*np.in1d(e.sector_id,[11,21,22,23,31,32,33,42,48,49]) + 4*np.in1d(e.sector_id,[7221,7222,7224]) + 5*np.in1d(e.sector_id,[44,45,7211,7212,7213,7223]) + 6*np.in1d(e.sector_id,[51,52,53,54,55,56,62,81,92])


@orca.column('establishments', 'sector_id_retail_agg', cache=True)
def sector_id_retail_agg(establishments):
    e = establishments
    return e.sector_id*np.logical_not(np.in1d(e.sector_id,[7211,7212,7213])) + 7211*np.in1d(e.sector_id,[7211,7212,7213])

