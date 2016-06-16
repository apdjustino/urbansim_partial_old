import orca
import pandas as pd
import numpy as np
import drcog_utils


from urbansim.models import TabularTotalsTransition, RelocationModel, TabularGrowthRateTransition

@orca.injectable('year')
def year(iter_var):
    return iter_var

@orca.step('emp_transition')
def emp_transition(employment_control_totals, year):
    location_fname = 'zone_id'
    tran = TabularTotalsTransition(employment_control_totals.to_frame(), 'total_number_of_jobs', accounting_column='employees')
    df = orca.merge_tables('establishments', tables=['establishments', 'counties'])
    print "%d establishments with %d employees before transition" % (len(df.index), df.employees.sum())
    df, added, copied, removed = tran.transition(df, year)
    print "%d establishments with %d employees after transition" % (len(df.index), df.employees.sum())
    df.loc[added, location_fname] = -1
    df.loc[added, "building_id"] = -1
    orca.add_table('establishments', df.loc[:, orca.get_table('establishments').local_columns])
@orca.step('emp_relocation')
def emp_relocation(establishments, job_relocation_rates):

    field_name = 'zone_id'
    print "Total agents: %d" % len(establishments)

    drcog_utils._print_number_unplaced(establishments, field_name)
    print "Assigning for relocation..."

    emp_relo_model = RelocationModel(job_relocation_rates.to_frame(), rate_column='job_relocation_probability')
    movers = emp_relo_model.find_movers(establishments.to_frame())

    print "%d agents relocating" % len(movers)


@orca.step('hh_transition')
def hh_transition(households, household_control_totals, year):
    location_fname = 'zone_id'
    tran = TabularTotalsTransition(household_control_totals.to_frame(), 'households')
    #tran = TabularGrowthRateTransition(household_control_totals.to_frame(), 'annual_rate')
    df = orca.merge_tables('households', tables=['households', 'counties'])
    print "{0} households with a total population of {1}".format(len(df.index), df.persons.sum())
    df, added, copied, removed = tran.transition(df, year)
    print "{0} households with a total population of {1}".format(len(df.index), df.persons.sum())
    df.loc[added, location_fname] = -1
    df.loc[added, 'building_id'] = -1
    orca.add_table('households', df.loc[:, orca.get_table('households').local_columns])

    print df.groupby('county_id').persons.sum()