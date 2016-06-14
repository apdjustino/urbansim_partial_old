import pandas as pd
import numpy as np


def split_function(parcels, selected_zones, demo_ids=None):

    pcoord = parcels.loc[(parcels.zone_id.isin(list(selected_zones.zone_id))) | (parcels.index.isin(demo_ids))]

    num_points = (np.ceil(pcoord['parcel_sqft']/200000)).astype('int32')
    num_points = num_points[num_points >1]
    pcoord_multi = pcoord.loc[num_points.index]
    pcoord_sqft = pcoord_multi.parcel_sqft / num_points

    id_list = []

    def add(id):
        for i in range(num_points[id] - 1):
            id_list.append(id)


    num_points.index.map(add)

    p = parcels.copy()
    p.loc[pcoord_sqft.index, 'parcel_sqft'] = pcoord_sqft.values
    new_parcels = p.loc[id_list]
    n = parcels.index.max()
    new_parcels.index = np.arange(n+1, n + len(new_parcels)+1)
    new_parcels.index.name = 'parcel_id'

    return pd.concat([p, new_parcels])



