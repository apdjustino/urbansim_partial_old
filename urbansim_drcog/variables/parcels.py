import orca
import pandas as pd
import numpy as np
from urbansim.utils.misc import reindex


@orca.column('parcels', 'in_denver', cache=True)
def in_denver(parcels):
    return (parcels.county_id==8031).astype('int32')

@orca.column('parcels', 'ln_dist_bus', cache=True)
def ln_dist_bus(parcels):
    return parcels.dist_bus.apply(np.log1p)

@orca.column('parcels', 'ln_dist_rail', cache=True)
def ln_dist_rail(parcels):
    return parcels.dist_rail.apply(np.log1p)

@orca.column('parcels', 'ln_land_value', cache=True)
def ln_land_value(parcels):
    return parcels.land_value.apply(np.log1p)

@orca.column('parcels', 'land_value_per_sqft', cache=True)
def land_value_per_sqft(parcels):
    return (parcels.land_value*1.0/parcels.parcel_sqft)

@orca.column('parcels', 'rail_within_mile', cache=True)
def rail_within_mile(parcels):
    return (parcels.dist_rail<5280).astype('int32')

@orca.column('parcels', 'cherry_creek_school_district', cache=True)
def cherry_creek_school_district(parcels):
    return (parcels.school_district == 8).astype('int32')

@orca.column('parcels', 'acres', cache=True)
def acres(parcels):
    return parcels.parcel_sqft/43560.0

@orca.column('parcels', 'ln_acres', cache=True)
def ln_acres(parcels):
    return (parcels.parcel_sqft/43560.0).apply(np.log1p)

@orca.column('parcels', 'ln_units_per_acre', cache=True)
def ln_units_per_acre(buildings, parcels):
    return (buildings.residential_units.groupby(buildings.parcel_id).sum()/parcels.acres).apply(np.log1p)

@orca.column('parcels', 'nonres_far', cache=True)
def nonres_far(buildings, parcels):
    return (buildings.non_residential_sqft.groupby(buildings.parcel_id).sum()/parcels.acres).apply(np.log1p)

@orca.column('parcels','parcel_size', cache=True)
def land_cost(parcels):
    return parcels.parcel_sqft

@orca.column('parcels','land_cost', cache=True)
def land_cost(parcels):
    return parcels.land_value

@orca.column('parcels', 'ave_res_unit_size')
def ave_unit_size(parcels, buildings):
    building_zone = pd.Series(index=buildings.index)
    b_zone_id = parcels.zone_id.loc[buildings.parcel_id]
    building_zone.loc[:] = b_zone_id.values
    series = pd.Series(index=parcels.index)
    zonal_sqft_per_unit = buildings.sqft_per_unit.groupby(building_zone).mean()
    series.loc[:] = zonal_sqft_per_unit[parcels.zone_id].values
    series = series.fillna(1500)
    return series

@orca.column('parcels', 'ave_non_res_unit_size')
def ave_unit_size(parcels, sqft_per_job):
    series = pd.Series(index=parcels.index)
    sqft = sqft_per_job.to_frame().reset_index()
    zonal_sqft = sqft.groupby('zone_id').building_sqft_per_job.mean()
    series.loc[:] = zonal_sqft[parcels.zone_id].values
    series = series.fillna(400)
    return series


@orca.column('parcels', 'total_units', cache=True, cache_scope='iteration')
def total_units(parcels, buildings):
    return buildings.residential_units.groupby(buildings.parcel_id).sum().\
        reindex(parcels.index).fillna(0)

@orca.column('parcels', 'total_job_spaces', cache=True, cache_scope='iteration')
def total_job_spaces(parcels, buildings):
    return buildings.non_residential_units.groupby(buildings.parcel_id).sum().\
        reindex(parcels.index).fillna(0)

