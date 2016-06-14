import orca
import pandas as pd
import numpy as np
import scp


######This module registers the necessary data tables into the orca pipeline#######
orca.add_injectable("store",pd.HDFStore('C:/urbansim2/urbansim/urbansim_drcog/data/drcog.h5', mode='r'))

@orca.table('counties', cache=True)
def counties():
    return pd.read_csv('C:/urbansim2/urbansim/urbansim_drcog/data/TAZ_County_Table.csv').set_index('zone_id')

@orca.table('zone_redevelopment', cache=True)
def zone_redevelopment():
    return pd.read_csv('C:/urbansim2/urbansim/urbansim_drcog/data/zone_redevelopments.csv')

@orca.table('buildings', cache=True)
def buildings(store, zone_redevelopment):
    b = store['buildings']
    b_removed_idx = b.loc[np.in1d(b.parcel_id, zone_redevelopment.parcel_id)].index
    return b.loc[~np.in1d(b.index, b_removed_idx)]

@orca.table('parcels', cache=True)
def parcels(store, zone_redevelopment):

    p = pd.read_csv('C:/urbansim2/urbansim/urbansim_drcog/data/parcels_fars.csv').set_index('parcel_id').rename(columns={'far':'new_far'})
    zone_dev = pd.read_csv('C:/urbansim2/urbansim/urbansim_drcog/data/zone_development.csv')
    parcels_with_new = scp.split_function(p, zone_dev, demo_ids=zone_redevelopment.parcel_id)
    return parcels_with_new

@orca.table('households', cache=True)
def households(store, zone_redevelopment):
    hh = store['households']
    b = store['buildings']
    b_removed_idx = b.loc[np.in1d(b.parcel_id, zone_redevelopment.parcel_id)].index
    hh.loc[np.in1d(b_removed_idx, hh.building_id), 'zone_id'] = -1
    hh.loc[np.in1d(b_removed_idx, hh.building_id), 'building_id'] = -1


    return hh

@orca.table('establishments', cache=True)
def establishments(store, zone_redevelopment):
    e = store['establishments']
    b = store['buildings']
    b_removed_idx = b.loc[np.in1d(b.parcel_id, zone_redevelopment.parcel_id)].index
    zone_ids = orca.merge_tables('buildings', tables=['buildings','parcels'], columns=['zone_id']).zone_id
    e = e.loc[e.employees > 0]
    e.loc[e.building_id.isin(b_removed_idx), 'building_id'] = -1
    e.loc[:, 'zone_id'] = zone_ids[e.building_id].fillna(-1).values.astype('int32')
    e.zone_id.fillna(-1)
    return e

@orca.table('zones', cache=True)
def zones(store):
    return store['zones']

@orca.table('sqft_per_job', cache=True)
def sqft_per_job(store):
    return store['building_sqft_per_job']

@orca.table('travel_data', cache=True)
def travel_data(store):
    # store['travel_data'].reset_index().set_index('gid')
    return store['travel_data']

@orca.table('scheduled_development_events', cache=True)
def scheduled_development_events(store):
    #if settings['urbancanvas']:
        from urbansim.urbancanvas import urbancanvas2
        print 'Loading development projects from Urban Canvas for scheduled_development_events'
        df = urbancanvas2.get_development_projects()
        ##Add any additional needed columns here
        df['note'] = 'Scheduled development event'
        for col in ['improvement_value', 'res_price_per_sqft', 'nonres_rent_per_sqft']:
            df[col] = 0
    #else:
        #print 'Loading scheduled_development_events from h5'
        #df = store['scheduled_development_events']
        return df

@orca.table('household_relocation_rates', cache=True)
def household_relocation_rates(store):
   return store['annual_household_relocation_rates']

@orca.table('job_relocation_rates', cache=True)
def job_relocation_rates(store):
    return store['annual_job_relocation_rates']

@orca.table('household_control_totals', cache=True)
def household_control_totals():
    return ''

@orca.table('employment_control_totals', cache=True)
def employment_control_totals():
    return pd.read_csv('C:/urbansim2/urbansim/urbansim_drcog/data/emp_growth.csv', index_col=0)

@orca.table('zoning', cache=True)
def zoning():
    return ''

@orca.table('t_data_dist20', cache=True)
def t_data_dist20( travel_data):
    t_data=travel_data.to_frame(columns=['am_single_vehicle_to_work_travel_time']).reset_index(level=1)
    return t_data[['to_zone_id']][t_data.am_single_vehicle_to_work_travel_time<20]

@orca.table('', cache=True)
def t_data_dist30( travel_data):
    t_data=travel_data.to_frame(columns=['am_single_vehicle_to_work_travel_time']).reset_index(level=1)
    return t_data[['to_zone_id']][t_data.am_single_vehicle_to_work_travel_time<30]

@orca.table('t_data_dist15', cache=True)
def t_data_dist15( travel_data):
    t_data=travel_data.to_frame(columns=['am_single_vehicle_to_work_travel_time']).reset_index(level=1)
    return t_data[['to_zone_id']][t_data.am_single_vehicle_to_work_travel_time<15]

@orca.table('t_data_dist45', cache=True)
def t_data_dist45( travel_data):
    t_data=travel_data.to_frame(columns=['am_single_vehicle_to_work_travel_time']).reset_index(level=1)
    return t_data[['to_zone_id']][t_data.am_single_vehicle_to_work_travel_time<45]

orca.broadcast('zones', 'parcels', cast_index=True, onto_on='zone_id')
orca.broadcast('parcels','buildings', cast_index=True, onto_on='parcel_id', onto_index=False)
orca.broadcast('buildings','households', cast_index=True, onto_on='building_id')
orca.broadcast('buildings', 'establishments', cast_index=True, onto_on ='building_id')
orca.broadcast('zoning', 'parcels', cast_index=True, onto_on='zoning_id')
orca.broadcast('fars', 'parcels', cast_index=True, onto_on='far_id')
orca.broadcast('counties','zones', cast_index=True, onto_index=True)
orca.broadcast('buildings','households_for_estimation', cast_index=True, onto_on='building_id')
orca.broadcast('counties', 'establishments', cast_index=True, onto_on='zone_id')
orca.broadcast('counties', 'households', cast_index=True, onto_on='zone_id')
