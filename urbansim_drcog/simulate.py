import orca
import models
import dataset
import buildings
import establishments
import households
import parcels
import zones


#orca.run(['hh_transition'], iter_vars=[2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021,2022,2023,2024,2025,2026,2027,2028,2029,2030,2031,2032,2033,2034,2035,2036,2037,2038,2039,2040])
orca.run(['hh_relocation'], iter_vars=[2040])