import orca
import pandas as pd
import numpy as np
from urbansim.utils.misc import reindex
import dataset
import zones
import households
import establishments
import parcels

@orca.column('buildings', 'vacant_residential_units')
def vacant_residential_units(buildings, households):
    return buildings.residential_units.sub(
        households.building_id.value_counts(), fill_value=0)

@orca.column('buildings', 'vacant_job_spaces')
def vacant_residential_units(buildings, establishments):
    return buildings.non_residential_units.sub(
        establishments.employees.groupby(establishments.building_id).sum(), fill_value=0)
#
@orca.column('buildings', 'non_residential_units', cache=True, cache_scope='iteration')
def non_residential_units(sqft_per_job, establishments):
    b = orca.merge_tables('buildings', tables=['buildings','parcels'], columns=['zone_id', 'building_type_id','non_residential_sqft'])
    b = pd.merge(b, sqft_per_job.to_frame(), left_on=[b.zone_id,b.building_type_id], right_index=True, how='left')
    b.loc[:, 'non_residential_units'] = np.ceil((b.non_residential_sqft / b.building_sqft_per_job).fillna(0)).astype('int')
    b.loc[:, 'base_year_jobs'] = establishments.employees.groupby(establishments.building_id).sum()
    return b[['non_residential_units', 'base_year_jobs']].max(axis=1)


@orca.column('buildings','townhome', cache=True)
def townhome(buildings):
    return (buildings.building_type_id == 24).astype('int32')

@orca.column('buildings','multifamily', cache=True)
def multifamily(buildings):
    return (np.in1d(buildings.building_type_id, [2,3])).astype('int32')

@orca.column('buildings', 'office', cache=True)
def office(buildings):
    return (buildings.building_type_id==5).astype('int32')

@orca.column('buildings', 'retail_or_restaurant', cache=True)
def retail_or_restaurant(buildings):
    return (np.in1d(buildings.building_type_id, [17,18])).astype('int32')

@orca.column('buildings', 'industrial_building', cache=True)
def industrial_building(buildings):
    return (np.in1d(buildings.building_type_id, [9,22])).astype('int32')

@orca.column('buildings', 'residential_sqft', cache=True)
def residential_sqft(buildings):
    return (buildings.bldg_sq_ft - buildings.non_residential_sqft)

@orca.column('buildings', 'btype_hlcm', cache=True)
def btype_hlcm(buildings):
    return 1*(buildings.building_type_id==2) + 2*(buildings.building_type_id==3) + 3*(buildings.building_type_id==20) + 4*np.invert(np.in1d(buildings.building_type_id,[2,3,20]))

@orca.column('buildings','county8001', cache=True, cache_scope='iteration')
def county8001(buildings):
    df = orca.merge_tables('buildings', tables=['buildings','parcels'], columns=['county_id'])
    return (df.county_id == 8001).astype('int32')

@orca.column('buildings','county8005', cache=True, cache_scope='iteration')
def county8005(buildings):
    df = orca.merge_tables('buildings', tables=['buildings','parcels'], columns=['county_id'])
    return (df.county_id == 8005).astype('int32')

@orca.column('buildings','county8013', cache=True, cache_scope='iteration')
def county8013(buildings):
    df = orca.merge_tables('buildings', tables=['buildings','parcels'], columns=['county_id'])
    return (df.county_id == 8013).astype('int32')

@orca.column('buildings','county8014', cache=True, cache_scope='iteration')
def county8014(buildings):
    df = orca.merge_tables('buildings', tables=['buildings','parcels'], columns=['county_id'])
    return (df.county_id == 8014).astype('int32')

@orca.column('buildings','county8019', cache=True, cache_scope='iteration')
def county8019(buildings):
    df = orca.merge_tables('buildings', tables=['buildings','parcels'], columns=['county_id'])
    return (df.county_id == 8019).astype('int32')

@orca.column('buildings','county8031', cache=True, cache_scope='iteration')
def county8031(buildings):
    df = orca.merge_tables('buildings', tables=['buildings','parcels'], columns=['county_id'])
    return (df.county_id == 8031).astype('int32')

@orca.column('buildings','county8035', cache=True, cache_scope='iteration')
def county8035(buildings):
    df = orca.merge_tables('buildings', tables=['buildings','parcels'], columns=['county_id'])
    return (df.county_id == 8035).astype('int32')

@orca.column('buildings','county8039', cache=True, cache_scope='iteration')
def county8039(buildings):
    df = orca.merge_tables('buildings', tables=['buildings','parcels'], columns=['county_id'])
    return (df.county_id == 8039).astype('int32')

@orca.column('buildings','county8047', cache=True, cache_scope='iteration')
def county8047(buildings):
    df = orca.merge_tables('buildings', tables=['buildings','parcels'], columns=['county_id'])
    return (df.county_id == 8047).astype('int32')

@orca.column('buildings','county8059', cache=True, cache_scope='iteration')
def county8059(buildings):
    df = orca.merge_tables('buildings', tables=['buildings','parcels'], columns=['county_id'])
    return (df.county_id == 8059).astype('int32')

@orca.column('buildings','county8123', cache=True, cache_scope='iteration')
def county8123(buildings):
    df = orca.merge_tables('buildings', tables=['buildings','parcels'], columns=['county_id'])
    return (df.county_id == 8123).astype('int32')


@orca.column('buildings', 'percent_hh_with_child_x_hh_with_child', cache=True, cache_scope='iteration')
def percent_hh_with_child_x_hh_with_child(buildings, households, parcels, zones):

    percent_hh_with_child = zones.percent_hh_with_child
    b_zone_ids = reindex(parcels.zone_id, buildings.parcel_id)
    hh_zones = pd.Series(b_zone_ids.loc[households.building_id].values, index=households.index)
    percent_hh_with_child_x_hh_with_child_test = percent_hh_with_child * households.hh_with_child.groupby(hh_zones).size()
    series = pd.Series(percent_hh_with_child_x_hh_with_child_test[b_zone_ids].values, index=buildings.index)

    return series

@orca.column('buildings', 'employees_x_ln_non_residential_sqft_zone', cache=True, cache_scope='iteration')
def employees_x_ln_non_residential_sqft_zone(buildings, establishments, parcels, zones):
    b_zone_ids = reindex(parcels.zone_id, buildings.parcel_id)
    e_zone_ids = pd.Series(b_zone_ids.loc[establishments.building_id].values, index=establishments.index)
    employees = establishments.employees.groupby(e_zone_ids).sum()

    ln_non_residential_sqft_zone = buildings.non_residential_sqft.groupby(b_zone_ids).sum().apply(np.log1p)
    employees_x_ln_non_residential_sqft_zone = employees * ln_non_residential_sqft_zone

    return pd.Series(employees_x_ln_non_residential_sqft_zone[b_zone_ids].values, index=buildings.index)


@orca.column('buildings', 'wkrs_hhs_x_ln_jobs_within_30min', cache=True, cache_scope='iteration')
def wkrs_hhs_x_ln_jobs_within_30min(buildings, households, parcels, zones):
    building_data = pd.DataFrame(index=buildings.index)
    p_zone_id = parcels.zone_id
    b_zone_id = p_zone_id.loc[buildings.parcel_id]
    building_data.loc[:, 'zone_id'] = b_zone_id.values
    building_data.loc[:, 'ln_jobs_within_30min'] = zones.ln_jobs_within_30min.loc[building_data.zone_id].values
    wkrs_hhs = households.wkrs_hhs.groupby(households.building_id).sum()
    return wkrs_hhs * building_data.ln_jobs_within_30min

@orca.column('buildings', 'ln_income_x_average_resunit_size', cache=True, cache_scope='iteration')
def ln_income_x_average_resunit_size(households, buildings, parcels):
    building_data = pd.Series(index=buildings.index)
    p_zone_id = parcels.zone_id
    b_zone_id = p_zone_id.loc[buildings.parcel_id]
    building_data.loc[:] = b_zone_id.values
    hh_data = pd.Series(index=households.index)
    hh_zone_id = building_data.loc[households.building_id]
    hh_data.loc[:] = hh_zone_id.values

    ln_income = households.ln_income.groupby(hh_data).mean()
    avg_resunit_size = buildings.sqft_per_unit.groupby(building_data).mean()
    ln_income_x_average_resunit_size = ln_income * avg_resunit_size
    return reindex(ln_income_x_average_resunit_size,building_data)

@orca.column('buildings', 'percent_younghead_x_younghead', cache=True, cache_scope='iteration')
def percent_younghead_x_younghead(buildings, households, zones,parcels):
    building_data = pd.Series(index=buildings.index)
    p_zone_id = parcels.zone_id
    b_zone_id = p_zone_id.loc[buildings.parcel_id]
    building_data.loc[:] = b_zone_id.values
    hh_data = pd.Series(index=households.index)
    hh_zone_id = building_data.loc[households.building_id]
    hh_data.loc[:] = hh_zone_id.values

    percent_younghead = zones.percent_younghead
    younghead = households.age_of_head.groupby(hh_data).size()
    percent_younghead_x_younghead = percent_younghead * younghead
    return reindex(percent_younghead_x_younghead, building_data)
