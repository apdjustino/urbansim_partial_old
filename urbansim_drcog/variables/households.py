import orca
import pandas as pd
import numpy as np
from urbansim.utils.misc import reindex
import dataset
import zones
import establishments
import parcels
import buildings





@orca.column('households', 'btype')
def btype(households):
    df = orca.merge_tables('households', tables=['households','buildings'], columns=['building_type_id'])
    return 1*(df.building_type_id==2) + 2*(df.building_type_id==3) + 3*(df.building_type_id==20) + 4*np.invert(np.in1d(df.building_type_id,[2,3,20]))


@orca.column('households', 'income_3_tenure', cache=True, cache_scope='iteration')
def income_3_tenure(households):
    return 1 * (households.income < 60000)*(households.tenure == 1) + 2 * np.logical_and(households.income >= 60000, households.income < 120000)*(households.tenure == 1) + 3*(households.income >= 120000)*(households.tenure == 1) + 4*(households.income < 40000)*(households.tenure == 2) + 5*(households.income >= 40000)*(households.tenure == 2)


@orca.column('households', 'younghead', cache=True)
def younghead(households):
    return households.age_of_head<30

@orca.column('households', 'hh_with_child', cache=True)
def hh_with_child(households):
    return households.children>0

@orca.column('households', 'ln_income', cache=True, cache_scope='iteration')
def ln_income(households):
    return households.income.apply(np.log1p)

@orca.column('households', 'income5xlt', cache=True, cache_scope='iteration')
def income5xlt(households):
    return households.income*5.0

@orca.column('households', 'income10xlt', cache=True, cache_scope='iteration')
def income10xlt(households):
    return households.income*5.0

@orca.column('households', 'wkrs_hhs', cache=True, cache_scope='iteration')
def wkrs_hhs(households):
    return households.workers*1.0/households.persons