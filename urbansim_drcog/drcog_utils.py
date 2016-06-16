import numpy as np
import pandas as pd
import orca
from urbansim.utils import misc
from urbansim.utils import yamlio
from urbansim.models import RegressionModel, SegmentedRegressionModel, MNLDiscreteChoiceModel, SegmentedMNLDiscreteChoiceModel
import os

def get_run_filename():
    return os.path.join(misc.runs_dir(), "run%d.h5" % misc.get_run_number())


def change_store(store_name):
    orca.add_injectable(
        "store",
        pd.HDFStore(os.path.join(misc.data_dir(), store_name), mode="r"))


def change_scenario(scenario):
    assert scenario in orca.get_injectable("scenario_inputs"), \
        "Invalid scenario name"
    print "Changing scenario to '%s'" % scenario
    orca.add_injectable("scenario", scenario)


def conditional_upzone(scenario, attr_name, upzone_name):
    scenario_inputs = orca.get_injectable("scenario_inputs")
    zoning_baseline = orca.get_table(
        scenario_inputs["baseline"]["zoning_table_name"])
    attr = zoning_baseline[attr_name]
    if scenario != "baseline":
        zoning_scenario = orca.get_table(
            scenario_inputs[scenario]["zoning_table_name"])
        upzone = zoning_scenario[upzone_name].dropna()
        attr = pd.concat([attr, upzone], axis=1).max(skipna=True, axis=1)
    return attr


def enable_logging():
    from urbansim.utils import logutil
    logutil.set_log_level(logutil.logging.INFO)
    logutil.log_to_stream()


def deal_with_nas(df):
    df_cnt = len(df)
    fail = False

    df = df.replace([np.inf, -np.inf], np.nan)
    for col in df.columns:
        s_cnt = df[col].count()
        if df_cnt != s_cnt:
            fail = True
            print "Found %d nas or inf (out of %d) in column %s" % \
                  (df_cnt-s_cnt, df_cnt, col)

    assert not fail, "NAs were found in dataframe, please fix"
    return df


def fill_nas_from_config(dfname, df):
    df_cnt = len(df)
    fillna_config = orca.get_injectable("fillna_config")
    fillna_config_df = fillna_config[dfname]
    for fname in fillna_config_df:
        filltyp, dtyp = fillna_config_df[fname]
        s_cnt = df[fname].count()
        fill_cnt = df_cnt - s_cnt
        if filltyp == "zero":
            val = 0
        elif filltyp == "mode":
            val = df[fname].dropna().value_counts().idxmax()
        elif filltyp == "median":
            val = df[fname].dropna().quantile()
        else:
            assert 0, "Fill type not found!"
        print "Filling column {} with value {} ({} values)".\
            format(fname, val, fill_cnt)
        df[fname] = df[fname].fillna(val).astype(dtyp)
    return df


def to_frame(tables, cfg, additional_columns=[]):
    cfg = yaml_to_class(cfg).from_yaml(str_or_buffer=cfg)
    tables = [t for t in tables if t is not None]
    columns = misc.column_list(tables, cfg.columns_used()) + additional_columns
    if len(tables) > 1:
        df = orca.merge_tables(target=tables[0].name,
                               tables=tables, columns=columns)
    else:
        df = tables[0].to_frame(columns)
    try:
        df = deal_with_nas(df)
    except:
        df.fillna(0, inplace=True)
    return df


def yaml_to_class(cfg):
    import yaml
    model_type = yaml.load(open(cfg))["model_type"]
    return {
        "regression": RegressionModel,
        "segmented_regression": SegmentedRegressionModel,
        "discretechoice": MNLDiscreteChoiceModel,
        "segmented_discretechoice": SegmentedMNLDiscreteChoiceModel
    }[model_type]


def _print_number_unplaced(df, fieldname):
    print "Total currently unplaced: %d" % \
          df[fieldname].value_counts().get(-1, 0)