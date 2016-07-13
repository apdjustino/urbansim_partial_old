import numpy as np
import pandas as pd
import dataset
import establishments
import zones
import buildings
import parcels
import orca
from urbansim.models import MNLDiscreteChoiceModel


alts = orca.get_table('zones').to_frame().fillna(0)

choosers = orca.get_table('establishments').to_frame()

rhs = "ln_avg_nonres_unit_price_zone + ln_residential_unit_density_zone + ln_non_residential_sqft_zone + ln_jobs_within_20min + zonal_pop + acreage"
sample_size = 50

probability_mode = "single_chooser"

choice_mode = "aggregate"

elcm = MNLDiscreteChoiceModel(rhs, sample_size=sample_size, probability_mode=probability_mode, choice_mode=choice_mode,
                              choice_column='zone_id', name='Zonal ELCM Model', estimation_sample_size=5000)

results = elcm.fit(choosers, alts, current_choice='zone_id')
elcm.to_yaml('c:/urbansim2/urbansim/urbansim_drcog/config/zonal_elcm_yaml.yaml')