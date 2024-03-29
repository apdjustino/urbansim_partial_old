import numpy as np
import pandas as pd
import dataset
import establishments
import zones
import buildings
import parcels
import orca
from urbansim.models import SegmentedMNLDiscreteChoiceModel


establishments = orca.get_table('establishments').to_frame()
sample_index = np.random.choice(establishments.index, 3000, replace=False)
establishments = establishments.loc[sample_index]
establishments_for_estimation = establishments[(establishments.building_id>0)&(establishments.home_based_status==0)]


###define model parameters

sample_size = 2000
seg_col = 'sector_id_retail_agg'
prob_mode = 'single_chooser'
choice_mode = 'aggregate'
choice_col = 'zone_id'

default_model = 'ln_avg_nonres_unit_price_zone+non_residential_sqft_zone+ln_residential_unit_density_zone' \
                '+ln_avg_unit_price_zone+median_age_of_head+mean_income+jobs_within_45min+ln_jobs_within_30min'

alts_filter = ['non_residential_sqft > 0']

remove_alts = False

elcm = SegmentedMNLDiscreteChoiceModel(segmentation_col=seg_col, sample_size=sample_size,
                                       probability_mode=prob_mode, choice_mode=choice_mode,
                                       choice_column=choice_col, default_model_expr=default_model,
                                       alts_fit_filters=None)


#alts = orca.get_table('alts_elcm').to_frame()
alts = orca.get_table('buildings').to_frame().fillna(0)
choosers = establishments_for_estimation.fillna(0)


results = elcm.fit(choosers, alts, current_choice='building_id')
elcm.to_yaml('c:/urbansim2/urbansim/urbansim_drcog/config/elcm_yaml.yaml')


##define models
# model11 = 'ln_jobs_within_30min+ln_avg_nonres_unit_price_zone+median_year_built+ln_residential_unit_density_zone' \
#                 '+ln_pop_within_20min+nonres_far+office+retail_or_restaurant+industrial_building' \
#                 '+employees_x_ln_non_residential_sqft_zone+ln_dist_rail+rail_within_mile'
#
# model21 = 'ln_jobs_within_30min+ln_avg_nonres_unit_price_zone+median_year_built+ln_residential_unit_density_zone' \
#                 '+ln_pop_within_20min+nonres_far+office+retail_or_restaurant+industrial_building' \
#                 '+employees_x_ln_non_residential_sqft_zone+rail_within_mile'
#
# model22 = 'ln_jobs_within_30min+ln_avg_nonres_unit_price_zone+median_year_built+ln_residential_unit_density_zone' \
#                 '+ln_pop_within_20min+nonres_far+office+retail_or_restaurant+industrial_building' \
#                 '+employees_x_ln_non_residential_sqft_zone+rail_within_mile'
#
# model23 = 'ln_jobs_within_30min+ln_avg_nonres_unit_price_zone+median_year_built+ln_residential_unit_density_zone' \
#                 '+ln_pop_within_20min+nonres_far+office+retail_or_restaurant+industrial_building' \
#                 '+employees_x_ln_non_residential_sqft_zone+rail_within_mile'
#
# model31 = 'ln_jobs_within_30min+ln_avg_nonres_unit_price_zone+median_year_built+ln_residential_unit_density_zone' \
#                 '+ln_pop_within_20min+nonres_far+office+retail_or_restaurant+industrial_building' \
#                 '+employees_x_ln_non_residential_sqft_zone+rail_within_mile'
#
# model32 = 'ln_jobs_within_30min+ln_avg_nonres_unit_price_zone+median_year_built+ln_residential_unit_density_zone' \
#                 '+ln_pop_within_20min+nonres_far+office+retail_or_restaurant+industrial_building' \
#                 '+employees_x_ln_non_residential_sqft_zone+rail_within_mile'
#
# model33 = 'ln_jobs_within_30min+ln_avg_nonres_unit_price_zone+median_year_built+ln_residential_unit_density_zone' \
#                 '+ln_pop_within_20min+nonres_far+office+retail_or_restaurant+industrial_building' \
#                 '+employees_x_ln_non_residential_sqft_zone+rail_within_mile'
#
# model42 = 'ln_jobs_within_30min+ln_avg_nonres_unit_price_zone+median_year_built+ln_residential_unit_density_zone' \
#                 '+ln_pop_within_20min+nonres_far+office+retail_or_restaurant+industrial_building' \
#                 '+employees_x_ln_non_residential_sqft_zone+ln_emp_sector2_within_15min+rail_within_mile'
#
# model44 = 'ln_jobs_within_30min+ln_avg_nonres_unit_price_zone+median_year_built+ln_residential_unit_density_zone' \
#                 '+ln_pop_within_20min+nonres_far+office+retail_or_restaurant+industrial_building' \
#                 '+employees_x_ln_non_residential_sqft_zone+ln_emp_sector5_within_15min+rail_within_mile'
#
# model45 = 'ln_jobs_within_30min+ln_avg_nonres_unit_price_zone+median_year_built+ln_residential_unit_density_zone' \
#                 '+ln_pop_within_20min+nonres_far+office+retail_or_restaurant+industrial_building' \
#                 '+employees_x_ln_non_residential_sqft_zone+ln_emp_sector5_within_15min+rail_within_mile'
#
# model48 = 'ln_jobs_within_30min+ln_avg_nonres_unit_price_zone+median_year_built+ln_residential_unit_density_zone' \
#                 '+ln_pop_within_20min+nonres_far+office+retail_or_restaurant+industrial_building' \
#                 '+employees_x_ln_non_residential_sqft_zone+rail_within_mile'
#
# model49 = 'ln_jobs_within_30min+ln_avg_nonres_unit_price_zone+median_year_built+ln_residential_unit_density_zone' \
#                 '+ln_pop_within_20min+nonres_far+office+retail_or_restaurant+industrial_building' \
#                 '+employees_x_ln_non_residential_sqft_zone+rail_within_mile'
#
# model51 = 'ln_jobs_within_30min+ln_avg_nonres_unit_price_zone+median_year_built+ln_residential_unit_density_zone' \
#                 '+ln_pop_within_20min+nonres_far+office+retail_or_restaurant+industrial_building' \
#                 '+employees_x_ln_non_residential_sqft_zone+ln_emp_sector6_within_15min+rail_within_mile'
#
# model52 = 'ln_jobs_within_30min+ln_avg_nonres_unit_price_zone+median_year_built+ln_residential_unit_density_zone' \
#                 '+ln_pop_within_20min+nonres_far+office+retail_or_restaurant+industrial_building' \
#                 '+employees_x_ln_non_residential_sqft_zone+ln_emp_sector6_within_15min+rail_within_mile'
#
# model53 = 'ln_jobs_within_30min+ln_avg_nonres_unit_price_zone+median_year_built+ln_residential_unit_density_zone' \
#                 '+ln_pop_within_20min+nonres_far+office+retail_or_restaurant+industrial_building' \
#                 '+employees_x_ln_non_residential_sqft_zone+ln_emp_sector6_within_15min+rail_within_mile'
#
# model54 = 'ln_jobs_within_30min+ln_avg_nonres_unit_price_zone+median_year_built+ln_residential_unit_density_zone' \
#                 '+ln_pop_within_20min+nonres_far+office+retail_or_restaurant+industrial_building' \
#                 '+employees_x_ln_non_residential_sqft_zone+ln_emp_sector6_within_15min+rail_within_mile'
#
# model55 = 'ln_jobs_within_30min+ln_avg_nonres_unit_price_zone+median_year_built+ln_residential_unit_density_zone' \
#                 '+ln_pop_within_20min+nonres_far+office+retail_or_restaurant+industrial_building' \
#                 '+employees_x_ln_non_residential_sqft_zone+ln_emp_sector6_within_15min+rail_within_mile'
#
# model56 = 'ln_jobs_within_30min+ln_avg_nonres_unit_price_zone+median_year_built+ln_residential_unit_density_zone' \
#                 '+ln_pop_within_20min+nonres_far+office+retail_or_restaurant+industrial_building' \
#                 '+employees_x_ln_non_residential_sqft_zone+ln_emp_sector6_within_15min+rail_within_mile'
#
# model61 = 'ln_jobs_within_30min+ln_avg_nonres_unit_price_zone+median_year_built+ln_residential_unit_density_zone' \
#                 '+ln_pop_within_20min+nonres_far+office+retail_or_restaurant+industrial_building' \
#                 '+employees_x_ln_non_residential_sqft_zone+ln_emp_sector6_within_15min+rail_within_mile'
#
# model62 = 'ln_jobs_within_30min+ln_avg_nonres_unit_price_zone+median_year_built+ln_residential_unit_density_zone' \
#                 '+ln_pop_within_20min+nonres_far+office+retail_or_restaurant+industrial_building' \
#                 '+employees_x_ln_non_residential_sqft_zone+ln_emp_sector6_within_15min+rail_within_mile'
#
# model71 = 'ln_jobs_within_30min+ln_avg_nonres_unit_price_zone+median_year_built+ln_residential_unit_density_zone' \
#                 '+ln_pop_within_20min+nonres_far+office+retail_or_restaurant+industrial_building' \
#                 '+employees_x_ln_non_residential_sqft_zone+ln_emp_sector2_within_15min+rail_within_mile'
#
# model81 = 'ln_jobs_within_30min+ln_avg_nonres_unit_price_zone+median_year_built+ln_residential_unit_density_zone' \
#                 '+ln_pop_within_20min+nonres_far+office+retail_or_restaurant+industrial_building' \
#                 '+employees_x_ln_non_residential_sqft_zone+ln_emp_sector6_within_15min+rail_within_mile'
#
# model92 = 'ln_jobs_within_30min+ln_avg_nonres_unit_price_zone+median_year_built+ln_residential_unit_density_zone' \
#                 '+ln_pop_within_20min+nonres_far+office+retail_or_restaurant+industrial_building' \
#                 '+employees_x_ln_non_residential_sqft_zone+ln_emp_sector6_within_15min+rail_within_mile'
# model7211 = 'ln_jobs_within_30min+ln_avg_nonres_unit_price_zone+median_year_built+ln_residential_unit_density_zone' \
#                 '+ln_pop_within_20min+nonres_far+office+retail_or_restaurant+industrial_building' \
#                 '+employees_x_ln_non_residential_sqft_zone+ln_emp_sector5_within_15min+rail_within_mile'
#
# model7221 = 'ln_jobs_within_30min+ln_avg_nonres_unit_price_zone+median_year_built+ln_residential_unit_density_zone' \
#                 '+ln_pop_within_20min+nonres_far+office+retail_or_restaurant+industrial_building' \
#                 '+employees_x_ln_non_residential_sqft_zone+ln_emp_sector4_within_15min+rail_within_mile'
#
# model7222 = 'ln_jobs_within_30min+ln_avg_nonres_unit_price_zone+median_year_built+ln_residential_unit_density_zone' \
#                 '+ln_pop_within_20min+nonres_far+office+retail_or_restaurant+industrial_building' \
#                 '+employees_x_ln_non_residential_sqft_zone+ln_emp_sector4_within_15min+rail_within_mile'
#
# model7223 = 'ln_jobs_within_30min+ln_avg_nonres_unit_price_zone+median_year_built+ln_residential_unit_density_zone' \
#                 '+ln_pop_within_20min+nonres_far+office+retail_or_restaurant+industrial_building' \
#                 '+employees_x_ln_non_residential_sqft_zone+ln_emp_sector5_within_15min+rail_within_mile'
#
# model7224 = 'ln_jobs_within_30min+ln_avg_nonres_unit_price_zone+median_year_built+ln_residential_unit_density_zone' \
#                 '+ln_pop_within_20min+nonres_far+office+retail_or_restaurant+industrial_building' \
#                 '+employees_x_ln_non_residential_sqft_zone+ln_emp_sector4_within_15min+rail_within_mile'


# elcm.add_segment(11, model_expression=model11)
# elcm.add_segment(21, model_expression=model21)
# elcm.add_segment(22, model_expression=model22)
# elcm.add_segment(23, model_expression=model23)
# elcm.add_segment(31, model_expression=model31)
# elcm.add_segment(32, model_expression=model32)
# elcm.add_segment(33, model_expression=model33)
# elcm.add_segment(42, model_expression=model42)
# elcm.add_segment(44, model_expression=model44)
# elcm.add_segment(45, model_expression=model45)
# elcm.add_segment(48, model_expression=model48)
# elcm.add_segment(49, model_expression=model49)
# elcm.add_segment(51, model_expression=model51)
# elcm.add_segment(52, model_expression=model52)
# elcm.add_segment(53, model_expression=model53)
# elcm.add_segment(54, model_expression=model54)
# elcm.add_segment(55, model_expression=model55)
# elcm.add_segment(56, model_expression=model56)
# elcm.add_segment(61, model_expression=model61)
# elcm.add_segment(62, model_expression=model62)
# elcm.add_segment(71, model_expression=model71)
# elcm.add_segment(81, model_expression=model81)
# elcm.add_segment(92, model_expression=model92)
# elcm.add_segment(7211, model_expression=model7211)
# elcm.add_segment(7221, model_expression=model7221)
# elcm.add_segment(7223, model_expression=model7223)
# elcm.add_segment(7224, model_expression=model7224)

