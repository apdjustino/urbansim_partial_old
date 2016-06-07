import orca
import pandas as pd


######This module registers the necessary data tables into the orca pipeline#######
orca.add_injectable("store",pd.HDFStore('C:/urbansim2/urbansim/urbansim_drcog/data/drcog.h5', mode='r'))

@orca.table('buildings', cache=True)
def buildings(store):
    return store['buildings']

@orca.table('parcels', cache=True)
def parcels(store):
    return store['parcels'].set_index('parcel_id')

@orca.table('households', cache=False)
def households(store):
    return store['households']

@orca.table('establishments', cache=False)
def establishments(store):
    return store['establishments']

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
    return ''

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

