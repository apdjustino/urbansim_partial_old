import orca
import pandas as pd
import numpy as np


from urbansim.models import TabularTotalsTransition

@orca.injectable('year')
def year(iter_var):
    return iter_var

@orca.step('emp_transition')
def emp_transition(employment_control_totals, year):
    tran = TabularTotalsTransition(employment_control_totals.to_frame(), 'total_number_of_jobs', accounting_column='employees')
    df = orca.merge_tables('establishments', tables=['establishments', 'counties'])
    print "%d establishments with %d employees before transition" % (len(df.index), df.employees.sum())
    df, added, copied, removed = tran.transition(df, year)
    print "%d establishments with %d employees after transition" % (len(df.index), df.employees.sum())