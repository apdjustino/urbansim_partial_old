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
choosers = orca.get_table('households').to_frame().fillna(0)

# rhs = 'ln_avg_unit_price_zone + ln_residential_unit_density_zone + ln_jobs_within_20min + zonal_pop + acreage + ' \
#       'percent_sf + mean_income + median_year_built + percent_hh_with_child + percent_renter_hh_in_zone +' \
#       'percent_younghead'

rhs = 'ln_avg_unit_price_zone + mean_income + percent_sf + zonal_pop + acreage + ln_jobs_within_15min + median_year_built'

sample_size = 50

probability_mode = "single_chooser"

choice_mode = "aggregate"

hlcm = MNLDiscreteChoiceModel(rhs, sample_size=sample_size, probability_mode=probability_mode, choice_mode=choice_mode,
                              choice_column='zone_id', name='Zonal HLCM Model', estimation_sample_size=5000)

results = hlcm.fit(choosers, alts, current_choice='zone_id')
hlcm.to_yaml('c:/urbansim2/urbansim/urbansim_drcog/config/zonal_hlcm_yaml.yaml')