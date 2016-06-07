import orca
import pandas as pd
import numpy as np
from urbansim.utils.misc import reindex

@orca.column('zones', 'res_units_per_bldg', cache=False)
def res_units_per_bldg():
    df = orca.merge_tables('buildings', tables=['buildings','parcels'], columns=['zone_id','residential_units'])
    df = df.groupby('zone_id').residential_units.mean()
    return df

@orca.column('zones', 'zonal_hh', cache=True, cache_scope='iteration')
def zonal_hh():
    df = orca.get_table('households').to_frame(columns=['building_type_id','zone_id'])
    return df.groupby('zone_id').size()

@orca.column('zones', 'zonal_emp', cache=True, cache_scope='iteration')
def zonal_emp():
    df = orca.get_table('establishments').to_frame(columns=['employees','zone_id'])
    return df.groupby('zone_id').employees.sum()

@orca.column('zones', 'residential_sqft_zone', cache=True, cache_scope='iteration')
def residential_sqft_zone():
    df = orca.merge_tables('buildings', tables=['buildings','parcels'], columns=['residential_sqft','zone_id'])
    return df.groupby('zone_id').residential_sqft.sum()

@orca.column('zones', 'zonal_pop', cache=True, cache_scope='iteration')
def zonal_pop():
    df = orca.get_table('households').to_frame(columns=['persons','zone_id'])
    return df.groupby('zone_id').persons.sum()

@orca.column('zones', 'residential_units_zone', cache=True, cache_scope='iteration')
def residential_units_zone():
    df = orca.merge_tables('buildings', tables=['buildings','parcels'], columns=['residential_units','zone_id'])
    return df.groupby('zone_id').residential_units.sum()

@orca.column('zones', 'ln_residential_units_zone', cache=True, cache_scope='iteration')
def residential_units_zone():
    df = orca.merge_tables('buildings', tables=['buildings','parcels'], columns=['residential_units','zone_id'])
    return df.groupby('zone_id').residential_units.sum().apply(np.log1p)

@orca.column('zones', 'ln_residential_unit_density_zone', cache=True, cache_scope='iteration')
def ln_residential_unit_density_zone(zones):
    df = orca.merge_tables('buildings', tables=['buildings','parcels'], columns=['residential_units','zone_id'])
    return (df.groupby('zone_id').residential_units.sum()/zones.acreage).apply(np.log1p)

@orca.column('zones', 'residential_unit_density_zone', cache=False)
def residential_unit_density_zone(zones):
    df = orca.merge_tables('buildings', tables=['buildings','parcels'], columns=['residential_units','zone_id'])
    return (df.groupby('zone_id').residential_units.sum()/zones.acreage)


@orca.column('zones', 'non_residential_sqft_zone', cache=True, cache_scope='iteration')
def non_residential_sqft_zone():
    df = orca.merge_tables('buildings', tables=['buildings','parcels'], columns=['non_residential_sqft','zone_id'])
    return df.groupby('zone_id').non_residential_sqft.sum()

@orca.column('zones', 'ln_non_residential_sqft_zone', cache=True, cache_scope='iteration')
def ln_non_residential_sqft_zone():
    df = orca.merge_tables('buildings', tables=['buildings','parcels'], columns=['non_residential_sqft','zone_id'])
    return df.groupby('zone_id').non_residential_sqft.sum().apply(np.log1p)

@orca.column('zones', 'percent_sf', cache=True, cache_scope='iteration')
def ln_non_residential_sqft_zone():
    b = orca.merge_tables('buildings', tables=['buildings','parcels'], columns=['btype_hlcm','residential_units','zone_id'])
    return b[b.btype_hlcm==3].groupby('zone_id').residential_units.sum()*100.0/(b.groupby('zone_id').residential_units.sum())


@orca.column('zones', 'avg_unit_price_zone', cache=False)
def avg_unit_price_zone(zones):
    b = orca.merge_tables('buildings', tables=['buildings','parcels'], columns=['residential_units','improvement_value','unit_price_residential','zone_id'])
    b =  b[(b.residential_units>0)*(b.improvement_value>0)].groupby('zone_id').unit_price_residential.mean()
    out = pd.Series(index=zones.index, name='avg_unit_price_zone')
    out.loc[b.index] = b
    out.loc[out.isnull()] = b.mean()
    return out

@orca.column('zones', 'ln_avg_unit_price_zone', cache=True, cache_scope='iteration')
def avg_unit_price_zone(zones):
    return zones.avg_unit_price_zone.apply(np.log1p)


@orca.column('zones', 'avg_nonres_unit_price_zone', cache=False)
def avg_unit_price_zone():
    b = orca.merge_tables('buildings', tables=['buildings','parcels'], columns=['non_residential_sqft','improvement_value','unit_price_non_residential','zone_id'])

    return  b[(b.non_residential_sqft>0)*(b.improvement_value>0)].groupby('zone_id').unit_price_non_residential.mean()


@orca.column('zones', 'ln_avg_nonres_unit_price_zone', cache=True, cache_scope='iteration')
def avg_unit_price_zone(zones):
    return zones.avg_nonres_unit_price_zone.apply(np.log1p)

@orca.column('zones', 'median_age_of_head', cache=True, cache_scope='iteration')
def median_age_of_head():
    hh = orca.get_table('households').to_frame(columns=['age_of_head','zone_id'])
    return hh.groupby('zone_id').age_of_head.median()

@orca.column('zones', 'mean_income', cache=True, cache_scope='iteration')
def mean_income():
    hh = orca.get_table('households').to_frame(columns=['income','zone_id'])
    return hh.groupby('zone_id').income.mean()

@orca.column('zones', 'median_year_built', cache=True, cache_scope='iteration')
def median_year_built():
    b = orca.merge_tables('buildings', tables=['buildings','parcels'], columns=['year_built','zone_id'])
    return b.groupby('zone_id').year_built.median().astype('int32')

@orca.column('zones', 'ln_avg_land_value_per_sqft_zone', cache=True, cache_scope='iteration')
def ln_avg_land_value_per_sqft_zone(parcels):

    return parcels.land_value_per_sqft.groupby(parcels.zone_id).mean().apply(np.log1p)


@orca.column('zones', 'median_yearbuilt_post_1990', cache=True, cache_scope='iteration')
def median_yearbuilt_post_1990():
    b = orca.merge_tables('buildings', tables=['buildings','parcels'], columns=['year_built','zone_id'])
    return (b.groupby('zone_id').year_built.median()>1990).astype('int32')

@orca.column('zones', 'median_yearbuilt_pre_1950', cache=True, cache_scope='iteration')
def median_yearbuilt_pre_1950():
    b = orca.merge_tables('buildings', tables=['buildings','parcels'], columns=['year_built','zone_id'])
    return (b.groupby('zone_id').year_built.median()<1950).astype('int32')

@orca.column('zones', 'percent_hh_with_child', cache=True, cache_scope='iteration')
def percent_hh_with_child(zones):
    hh = orca.get_table('households').to_frame(columns=['children','zone_id'])
    return hh[hh.children>0].groupby('zone_id').size()*100.0/zones.zonal_hh

@orca.column('zones', 'percent_renter_hh_in_zone', cache=True, cache_scope='iteration')
def percent_renter_hh_in_zone(zones):
    hh = orca.get_table('households').to_frame(columns=['tenure','zone_id'])
    return hh[hh.tenure==2].groupby('zone_id').size()*100.0/zones.zonal_hh

@orca.column('zones', 'percent_younghead', cache=True, cache_scope='iteration')
def percent_younghead(zones):
    hh = orca.get_table('households').to_frame(['age_of_head','zone_id'])
    return hh[hh.age_of_head<30].groupby('zone_id').size()*100.0/zones.zonal_hh

@orca.column('zones', 'average_resunit_size', cache=True, cache_scope='iteration')
def average_resunit_size():
    b = orca.merge_tables('buildings', tables=['buildings','parcels'], columns=['sqft_per_unit','zone_id'])
    return  b.groupby('zone_id').sqft_per_unit.mean()



@orca.column('zones', 'emp_sector_agg', cache=True, cache_scope='iteration')
def emp_sector_agg():
    e = orca.get_table('establishments').to_frame(columns=['sector_id','employees','zone_id'])
    return  e[e.sector_id==1].groupby('zone_id').employees.sum()

@orca.column('zones', 'emp_sector1', cache=True, cache_scope='iteration')
def emp_sector1():
    e = orca.get_table('establishments').to_frame(columns=['sector_id_six','employees','zone_id'])
    return  e[e.sector_id_six==1].groupby('zone_id').employees.sum()

@orca.column('zones', 'emp_sector2', cache=True, cache_scope='iteration')
def emp_sector2():
    e = orca.get_table('establishments').to_frame(columns=['sector_id_six','employees','zone_id'])
    return  e[e.sector_id_six==2].groupby('zone_id').employees.sum()

@orca.column('zones', 'emp_sector3', cache=True, cache_scope='iteration')
def emp_sector3():
    e = orca.get_table('establishments').to_frame(columns=['sector_id_six','employees','zone_id'])
    return  e[e.sector_id_six==3].groupby('zone_id').employees.sum()

@orca.column('zones', 'emp_sector4', cache=True, cache_scope='iteration')
def emp_sector4():
    e = orca.get_table('establishments').to_frame(columns=['sector_id_six','employees','zone_id'])
    return  e[e.sector_id_six==4].groupby('zone_id').employees.sum()

@orca.column('zones', 'emp_sector5', cache=True, cache_scope='iteration')
def emp_sector5():
    e = orca.get_table('establishments').to_frame(columns=['sector_id_six','employees','zone_id'])
    return  e[e.sector_id_six==5].groupby('zone_id').employees.sum()

@orca.column('zones', 'emp_sector6', cache=True, cache_scope='iteration')
def emp_sector6():
    e = orca.get_table('establishments').to_frame(columns=['sector_id_six','employees','zone_id'])
    return  e[e.sector_id_six==6].groupby('zone_id').employees.sum()

@orca.column('zones', 'emp_sector6', cache=True, cache_scope='iteration')
def emp_sector6():
    e = orca.get_table('establishments').to_frame(columns=['sector_id_six','employees','zone_id'])
    return  e[e.sector_id_six==6].groupby('zone_id').employees.sum()

@orca.column('zones', 'jobs_within_45min', cache=True, cache_scope='iteration')
def jobs_within_45min(zones, t_data_dist45):
    b = orca.merge_tables('buildings', tables=['buildings','parcels'], columns=['zone_id'])
    t_data=t_data_dist45.to_frame()
    t_data.loc[:,'attr']=zones.zonal_emp[t_data_dist45.to_zone_id].values
    zone_time_range=t_data.groupby(level=0).attr.apply(np.sum)
    return reindex(zone_time_range, b.zone_id)

@orca.column('zones', 'ln_jobs_within_45min', cache=True, cache_scope='iteration')
def ln_jobs_within_45min(zones):
    return zones.jobs_within_45min.apply(np.log1p)

@orca.column('zones', 'jobs_within_30min', cache=True, cache_scope='iteration')
def jobs_within_30min(zones, t_data_dist30):
    b = orca.merge_tables('buildings', tables=['buildings','parcels'], columns=['zone_id'])
    t_data=t_data_dist30.to_frame()
    t_data.loc[:,'attr']=zones.zonal_emp[t_data_dist30.to_zone_id].values
    zone_time_range=t_data.groupby(level=0).attr.apply(np.sum)
    return reindex(zone_time_range, b.zone_id)

@orca.column('zones', 'ln_jobs_within_30min', cache=True, cache_scope='iteration')
def ln_jobs_within_30min(zones):
    return zones.jobs_within_30min.apply(np.log1p)

@orca.column('zones', 'jobs_within_20min', cache=True, cache_scope='iteration')
def jobs_within_20min(zones, t_data_dist20):
    b = orca.merge_tables('buildings', tables=['buildings','parcels'], columns=['zone_id'])
    t_data=t_data_dist20.to_frame()
    t_data.loc[:,'attr']=zones.zonal_emp[t_data_dist20.to_zone_id].values
    zone_time_range=t_data.groupby(level=0).attr.apply(np.sum)
    return reindex(zone_time_range, b.zone_id)

@orca.column('zones', 'ln_jobs_within_20min', cache=True, cache_scope='iteration')
def ln_jobs_within_20min(zones):
    return zones.jobs_within_20min.apply(np.log1p)

@orca.column('zones', 'jobs_within_15min', cache=True, cache_scope='iteration')
def jobs_within_15min(zones, t_data_dist15):
    b = orca.merge_tables('buildings', tables=['buildings','parcels'], columns=['zone_id'])
    t_data=t_data_dist15.to_frame()
    t_data.loc[:,'attr']=zones.zonal_emp[t_data_dist15.to_zone_id].values
    zone_time_range=t_data.groupby(level=0).attr.apply(np.sum)
    return reindex(zone_time_range, b.zone_id)

@orca.column('zones', 'ln_jobs_within_15min', cache=True, cache_scope='iteration')
def ln_jobs_within_15min(zones):
    return zones.jobs_within_15min.apply(np.log1p)

@orca.column('zones', 'ln_emp_sector1_within_15min', cache=True, cache_scope='iteration')
def ln_emp_sector1_within_15min(t_data_dist15):
    b = orca.merge_tables('buildings', tables=['buildings','parcels'], columns=['zone_id'])
    e =orca.get_table('establishments').to_frame(columns=['sector_id_six','zone_id','employees'])
    e = e.loc[e.sector_id_six == 1]
    zonal_emp = e.groupby('zone_id').employees.sum()
    t_data=t_data_dist15.to_frame()
    t_data.loc[:,'attr']=zonal_emp[t_data_dist15.to_zone_id].values
    zone_time_range=t_data.groupby(level=0).attr.apply(np.sum)
    return reindex(zone_time_range,b.zone_id).apply(np.log1p)

@orca.column('zones', 'ln_emp_sector2_within_15min', cache=True, cache_scope='iteration')
def ln_emp_sector2_within_15min(t_data_dist15):
    b = orca.merge_tables('buildings', tables=['buildings','parcels'], columns=['zone_id'])
    e =orca.get_table('establishments').to_frame(columns=['sector_id_six','zone_id','employees'])
    e = e.loc[e.sector_id_six == 2]
    zonal_emp = e.groupby('zone_id').employees.sum()
    t_data=t_data_dist15.to_frame()
    t_data.loc[:,'attr']=zonal_emp[t_data_dist15.to_zone_id].values
    zone_time_range=t_data.groupby(level=0).attr.apply(np.sum)
    return reindex(zone_time_range,b.zone_id).apply(np.log1p)

@orca.column('zones', 'ln_emp_sector3_within_15min', cache=True, cache_scope='iteration')
def ln_emp_sector4_within_15min(t_data_dist15):
    b = orca.merge_tables('buildings', tables=['buildings','parcels'], columns=['zone_id'])
    e =orca.get_table('establishments').to_frame(columns=['sector_id_six','zone_id','employees'])
    e = e.loc[e.sector_id_six == 3]
    zonal_emp = e.groupby('zone_id').employees.sum()
    t_data=t_data_dist15.to_frame()
    t_data.loc[:,'attr']=zonal_emp[t_data_dist15.to_zone_id].values
    zone_time_range=t_data.groupby(level=0).attr.apply(np.sum)
    return reindex(zone_time_range,b.zone_id).apply(np.log1p)

@orca.column('zones', 'ln_emp_sector4_within_15min', cache=True, cache_scope='iteration')
def ln_emp_sector4_within_15min(t_data_dist15):
    b = orca.merge_tables('buildings', tables=['buildings','parcels'], columns=['zone_id'])
    e =orca.get_table('establishments').to_frame(columns=['sector_id_six','zone_id','employees'])
    e = e.loc[e.sector_id_six == 4]
    zonal_emp = e.groupby('zone_id').employees.sum()
    t_data=t_data_dist15.to_frame()
    t_data.loc[:,'attr']=zonal_emp[t_data_dist15.to_zone_id].values
    zone_time_range=t_data.groupby(level=0).attr.apply(np.sum)
    return reindex(zone_time_range,b.zone_id).apply(np.log1p)

@orca.column('zones', 'ln_emp_sector5_within_15min', cache=True, cache_scope='iteration')
def ln_emp_sector5_within_15min(t_data_dist15):
    b = orca.merge_tables('buildings', tables=['buildings','parcels'], columns=['zone_id'])
    e =orca.get_table('establishments').to_frame(columns=['sector_id_six','zone_id','employees'])
    e = e.loc[e.sector_id_six == 5]
    zonal_emp = e.groupby('zone_id').employees.sum()
    t_data=t_data_dist15.to_frame()
    t_data.loc[:,'attr']=zonal_emp[t_data_dist15.to_zone_id].values
    zone_time_range=t_data.groupby(level=0).attr.apply(np.sum)
    return reindex(zone_time_range,b.zone_id).apply(np.log1p)

@orca.column('zones', 'ln_emp_sector6_within_15min', cache=True, cache_scope='iteration')
def ln_emp_sector6_within_15min(t_data_dist15):
    b = orca.merge_tables('buildings', tables=['buildings','parcels'], columns=['zone_id'])
    e =orca.get_table('establishments').to_frame(columns=['sector_id_six','zone_id','employees'])
    e = e.loc[e.sector_id_six == 6]
    zonal_emp = e.groupby('zone_id').employees.sum()
    t_data=t_data_dist15.to_frame()
    t_data.loc[:,'attr']=zonal_emp[t_data_dist15.to_zone_id].values
    zone_time_range=t_data.groupby(level=0).attr.apply(np.sum)
    return reindex(zone_time_range,b.zone_id).apply(np.log1p)

@orca.column('zones', 'ln_emp_sector3_within_20min', cache=True, cache_scope='iteration')
def ln_emp_sector3_within_20min(t_data_dist20):
    b = orca.merge_tables('buildings', tables=['buildings','parcels'], columns=['zone_id'])
    e =orca.get_table('establishments').to_frame(columns=['sector_id_six','zone_id','employees'])
    e = e.loc[e.sector_id_six == 3]
    zonal_emp = e.groupby('zone_id').employees.sum()
    t_data=t_data_dist20.to_frame()
    t_data.loc[:,'attr']=zonal_emp[t_data_dist20.to_zone_id].values
    zone_time_range=t_data.groupby(level=0).attr.apply(np.sum)
    return reindex(zone_time_range,b.zone_id).apply(np.log1p)

@orca.column('zones', 'ln_emp_sector5_within_20min', cache=True, cache_scope='iteration')
def ln_emp_sector5_within_20min(t_data_dist20):
    b = orca.merge_tables('buildings', tables=['buildings','parcels'], columns=['zone_id'])
    e =orca.get_table('establishments').to_frame(columns=['sector_id_six','zone_id','employees'])
    e = e.loc[e.sector_id_six == 5]
    zonal_emp = e.groupby('zone_id').employees.sum()
    t_data=t_data_dist20.to_frame()
    t_data.loc[:,'attr']=zonal_emp[t_data_dist20.to_zone_id].values
    zone_time_range=t_data.groupby(level=0).attr.apply(np.sum)
    return reindex(zone_time_range,b.zone_id).apply(np.log1p)


@orca.column('zones', 'allpurpose_agglosum_floor', cache=True, cache_scope='iteration')
def allpurpose_agglosum_floor(zones):
    return (zones.allpurpose_agglosum>=0)*(zones.allpurpose_agglosum)

@orca.column('zones', 'ln_pop_within_20min', cache=True, cache_scope='iteration')
def ln_pop_within_20min(zones, t_data_dist20):
    b = orca.merge_tables('buildings', tables=['buildings','parcels'], columns=['zone_id'])
    zonal_pop=zones.zonal_pop
    t_data=t_data_dist20.to_frame()
    t_data.loc[:,'attr']=zonal_pop[t_data_dist20.to_zone_id].values
    zone_time_range=t_data.groupby(level=0).attr.apply(np.sum)
